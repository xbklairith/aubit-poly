"""Position redeemer for claiming winning Polymarket positions.

This module handles redemption of winning positions through:
1. Direct CTF contract interaction (for EOA wallets)
2. Safe wallet execTransaction (for Polymarket proxy wallets)
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

import httpx
from eth_account import Account
from eth_account.signers.local import LocalAccount
from web3 import Web3
from web3.contract import Contract
from web3.types import TxParams, TxReceipt

from pylo.config.settings import get_settings

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Contract Addresses (Polygon Mainnet)
# ═══════════════════════════════════════════════════════════════════════════════

# Polymarket APIs
DATA_API_URL = "https://data-api.polymarket.com"
RELAYER_URL = "https://relayer-v2.polymarket.com"

# Conditional Tokens Framework - holds all prediction market positions
CTF_ADDRESS = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")

# USDC on Polygon - collateral token
USDC_ADDRESS = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

# Gnosis Safe Proxy Factory - for MetaMask users
SAFE_FACTORY_ADDRESS = Web3.to_checksum_address("0xaacFeEa03eb1561C4e67d661e40682Bd20e3541b")

# Safe Singleton (master copy)
SAFE_SINGLETON_ADDRESS = Web3.to_checksum_address("0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552")

# Null address
NULL_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# ═══════════════════════════════════════════════════════════════════════════════
# Contract ABIs (minimal - only functions we need)
# ═══════════════════════════════════════════════════════════════════════════════

CTF_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "payoutDenominator",
        "type": "function",
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "payoutNumerators",
        "type": "function",
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "outcomeIndex", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

SAFE_ABI = [
    {
        "name": "execTransaction",
        "type": "function",
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"},
        ],
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "payable",
    },
    {
        "name": "nonce",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "getTransactionHash",
        "type": "function",
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "_nonce", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view",
    },
    {
        "name": "getOwners",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
    },
]

# ═══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class RedemptionResult:
    """Result of a redemption attempt."""

    success: bool
    tx_hash: str | None = None
    payout_usdc: Decimal = Decimal("0")
    error: str | None = None
    gas_used: int = 0


@dataclass
class RedeemablePosition:
    """A position that can be redeemed from Polymarket API."""

    condition_id: str
    title: str
    outcome: str
    size: Decimal
    avg_price: Decimal
    pnl: Decimal
    proxy_wallet: str
    current_value: Decimal  # Value to redeem (0 for losers, size for winners)
    cur_price: Decimal  # 1 for winner, 0 for loser
    asset: str = ""

    @property
    def is_winner(self) -> bool:
        """Check if this is a winning position (has value to redeem)."""
        return self.current_value > 0

    @classmethod
    def from_api_response(cls, data: dict) -> "RedeemablePosition":
        """Create from API response."""
        return cls(
            condition_id=data.get("conditionId", ""),
            title=data.get("title", ""),
            outcome=data.get("outcome", ""),
            size=Decimal(str(data.get("size", 0))),
            avg_price=Decimal(str(data.get("avgPrice", 0))),
            pnl=Decimal(str(data.get("cashPnl", 0))),
            proxy_wallet=data.get("proxyWallet", ""),
            current_value=Decimal(str(data.get("currentValue", 0))),
            cur_price=Decimal(str(data.get("curPrice", 0))),
            asset=data.get("asset", ""),
        )


@dataclass
class MarketResolution:
    """Market resolution status from on-chain data."""

    condition_id: str
    is_resolved: bool
    winning_outcome: int | None = None  # 0 = YES, 1 = NO
    payout_denominator: int = 0


# ═══════════════════════════════════════════════════════════════════════════════
# Position Redeemer
# ═══════════════════════════════════════════════════════════════════════════════


class PositionRedeemer:
    """Redeems winning positions from Polymarket CTF contract."""

    def __init__(
        self,
        private_key: str | None = None,
        rpc_url: str | None = None,
        proxy_address: str | None = None,
        use_safe_wallet: bool = True,
    ):
        """Initialize the redeemer.

        Args:
            private_key: Wallet private key (with or without 0x prefix)
            rpc_url: Polygon RPC URL
            proxy_address: Polymarket proxy wallet address (overrides derivation)
            use_safe_wallet: If True, execute through Safe proxy wallet
        """
        settings = get_settings()

        # Get credentials
        self._private_key = private_key or settings.wallet_private_key.get_secret_value()
        self._rpc_url = rpc_url or settings.polygon_rpc_url
        self._proxy_address = proxy_address or settings.polymarket_wallet_address

        if not self._private_key:
            raise ValueError("Wallet private key is required")

        # Initialize Web3
        self.w3 = Web3(Web3.HTTPProvider(self._rpc_url))
        if not self.w3.is_connected():
            raise ConnectionError(f"Failed to connect to RPC: {self._rpc_url}")

        # Initialize account
        self.account: LocalAccount = Account.from_key(self._private_key)
        self.eoa_address = self.account.address

        # Use provided proxy address or derive Safe wallet address
        self.use_safe_wallet = use_safe_wallet
        self.safe_address: str | None = None
        self.safe_contract: Contract | None = None

        if self._proxy_address:
            # Use explicitly provided proxy address
            self.safe_address = Web3.to_checksum_address(self._proxy_address)
        elif use_safe_wallet:
            # Derive Safe address (may not match Polymarket's actual proxy)
            self.safe_address = self._derive_safe_address(self.eoa_address)

        # Initialize contracts
        self.ctf_contract: Contract = self.w3.eth.contract(address=CTF_ADDRESS, abi=CTF_ABI)
        if self.safe_address:
            self.safe_contract = self.w3.eth.contract(address=self.safe_address, abi=SAFE_ABI)

        logger.info(f"PositionRedeemer initialized - EOA: {self.eoa_address}")
        if self.safe_address:
            logger.info(f"Proxy wallet: {self.safe_address}")

    def _derive_safe_address(self, owner: str) -> str:
        """Derive Safe proxy address from owner EOA using CREATE2.

        Polymarket uses Gnosis Safe with a deterministic deployment.
        The Safe address is derived from: factory + singleton + owner + salt
        """
        owner = Web3.to_checksum_address(owner)

        # Safe initializer data: setup(owners, threshold, to, data, fallback, payment, paymentReceiver)
        # For 1-of-1 Safe: owners=[owner], threshold=1, everything else zeroed
        setup_abi = [
            {
                "name": "setup",
                "type": "function",
                "inputs": [
                    {"name": "_owners", "type": "address[]"},
                    {"name": "_threshold", "type": "uint256"},
                    {"name": "to", "type": "address"},
                    {"name": "data", "type": "bytes"},
                    {"name": "fallbackHandler", "type": "address"},
                    {"name": "paymentToken", "type": "address"},
                    {"name": "payment", "type": "uint256"},
                    {"name": "paymentReceiver", "type": "address"},
                ],
                "outputs": [],
                "stateMutability": "nonpayable",
            }
        ]
        setup_contract = self.w3.eth.contract(address=SAFE_SINGLETON_ADDRESS, abi=setup_abi)
        setup_data = setup_contract.encode_abi(
            "setup",
            args=[
                [owner],  # owners
                1,  # threshold
                NULL_ADDRESS,  # to
                b"",  # data
                NULL_ADDRESS,  # fallbackHandler
                NULL_ADDRESS,  # paymentToken
                0,  # payment
                NULL_ADDRESS,  # paymentReceiver
            ],
        )

        # Salt is keccak256 of the initializer
        salt = Web3.keccak(hexstr=setup_data)

        # Proxy creation code with singleton address
        # GnosisSafeProxy creation code + singleton address
        proxy_creation_code = bytes.fromhex(
            "608060405234801561001057600080fd5b506040516101e63803806101e68339818101604052602081101561003357600080fd5b8101908080519060200190929190505050600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff1614156100ca576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004018080602001828103825260228152602001806101c46022913960400191505060405180910390fd5b806000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505060ab806101196000396000f3fe608060405273ffffffffffffffffffffffffffffffffffffffff600054167fa619486e0000000000000000000000000000000000000000000000000000000060003514156050578060005260206000f35b3660008037600080366000845af43d6000803e60008114156070573d6000fd5b3d6000f3fea264697066735822122003d1488ee65e08fa41e58e888a9865554c535f2c77126a82cb4c0f917f31441a64736f6c63430007060033496e76616c69642073696e676c65746f6e20616464726573732070726f7669646564"
        )

        # Encode singleton address as constructor argument
        encoded_singleton = Web3.to_bytes(hexstr=SAFE_SINGLETON_ADDRESS).rjust(32, b"\x00")

        # Full init code = creation code + encoded constructor args
        init_code = proxy_creation_code + encoded_singleton

        # CREATE2 address calculation
        # address = keccak256(0xff ++ factory ++ salt ++ keccak256(init_code))[12:]
        init_code_hash = Web3.keccak(init_code)

        create2_input = (
            bytes.fromhex("ff") + Web3.to_bytes(hexstr=SAFE_FACTORY_ADDRESS) + salt + init_code_hash
        )

        safe_address = Web3.to_checksum_address(Web3.keccak(create2_input)[12:].hex())

        return safe_address

    def check_resolution(self, condition_id: str) -> MarketResolution:
        """Check if a market has resolved on-chain.

        Args:
            condition_id: The market's condition ID (bytes32 hex string)

        Returns:
            MarketResolution with resolution status
        """
        # Ensure proper bytes32 format
        if not condition_id.startswith("0x"):
            condition_id = "0x" + condition_id
        condition_id = condition_id.lower()

        condition_bytes = Web3.to_bytes(hexstr=condition_id)

        # Check payout denominator - if > 0, market is resolved
        payout_denom = self.ctf_contract.functions.payoutDenominator(condition_bytes).call()

        if payout_denom == 0:
            return MarketResolution(
                condition_id=condition_id,
                is_resolved=False,
            )

        # Get winning outcome (check numerators)
        # For binary markets: index 0 = YES, index 1 = NO
        yes_payout = self.ctf_contract.functions.payoutNumerators(condition_bytes, 0).call()
        no_payout = self.ctf_contract.functions.payoutNumerators(condition_bytes, 1).call()

        winning_outcome = None
        if yes_payout > 0:
            winning_outcome = 0  # YES won
        elif no_payout > 0:
            winning_outcome = 1  # NO won

        return MarketResolution(
            condition_id=condition_id,
            is_resolved=True,
            winning_outcome=winning_outcome,
            payout_denominator=payout_denom,
        )

    def get_position_balance(self, token_id: str, wallet_address: str | None = None) -> int:
        """Get position balance for a specific outcome token.

        Args:
            token_id: The outcome token ID
            wallet_address: Address to check (defaults to Safe or EOA)

        Returns:
            Balance in wei (divide by 10^6 for USDC equivalent shares)
        """
        address = wallet_address or self.safe_address or self.eoa_address
        address = Web3.to_checksum_address(address)

        # Token ID in CTF is the positionId
        position_id = int(token_id)

        balance = self.ctf_contract.functions.balanceOf(address, position_id).call()

        return balance

    def get_redeemable_positions(
        self, wallet_address: str | None = None
    ) -> list[RedeemablePosition]:
        """Fetch redeemable positions from Polymarket Data API.

        Args:
            wallet_address: Address to check (defaults to Safe or EOA)

        Returns:
            List of RedeemablePosition objects ready for redemption
        """
        address = wallet_address or self.safe_address or self.eoa_address

        try:
            response = httpx.get(
                f"{DATA_API_URL}/positions",
                params={
                    "user": address,
                    "redeemable": "true",
                    "limit": 500,
                },
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()

            positions = [RedeemablePosition.from_api_response(item) for item in data]

            logger.info(f"Found {len(positions)} redeemable position(s) for {address}")
            return positions

        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch redeemable positions: {e}")
            return []

    def get_all_positions(self, wallet_address: str | None = None) -> list[RedeemablePosition]:
        """Fetch all positions from Polymarket Data API.

        Args:
            wallet_address: Address to check (defaults to Safe or EOA)

        Returns:
            List of all RedeemablePosition objects (including non-redeemable)
        """
        address = wallet_address or self.safe_address or self.eoa_address

        try:
            response = httpx.get(
                f"{DATA_API_URL}/positions",
                params={
                    "user": address,
                    "limit": 500,
                },
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()

            positions = [RedeemablePosition.from_api_response(item) for item in data]

            logger.info(f"Found {len(positions)} total position(s) for {address}")
            return positions

        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch positions: {e}")
            return []

    def redeem_position(
        self,
        condition_id: str,
        dry_run: bool = True,
    ) -> RedemptionResult:
        """Redeem a winning position.

        Args:
            condition_id: The market's condition ID
            dry_run: If True, simulate without executing

        Returns:
            RedemptionResult with transaction details
        """
        # Validate condition ID format
        if not condition_id.startswith("0x"):
            condition_id = "0x" + condition_id

        condition_bytes = Web3.to_bytes(hexstr=condition_id)

        # Check if market is resolved
        resolution = self.check_resolution(condition_id)
        if not resolution.is_resolved:
            return RedemptionResult(
                success=False,
                error=f"Market {condition_id[:10]}... is not resolved yet",
            )

        logger.info(
            f"Market resolved - winning outcome: "
            f"{'YES' if resolution.winning_outcome == 0 else 'NO'}"
        )

        # Encode redeemPositions call
        # parentCollectionId = 0x0 (root collection for Polymarket)
        # indexSets = [1, 2] for binary markets (YES=1, NO=2)
        redeem_data = self.ctf_contract.encode_abi(
            "redeemPositions",
            args=[
                USDC_ADDRESS,  # collateralToken
                bytes(32),  # parentCollectionId (zeros)
                condition_bytes,  # conditionId
                [1, 2],  # indexSets for binary market
            ],
        )

        if dry_run:
            logger.info(f"[DRY RUN] Would redeem position for {condition_id[:10]}...")
            return RedemptionResult(
                success=True,
                error="Dry run - no transaction executed",
            )

        # Execute transaction
        if self.use_safe_wallet and self.safe_address:
            return self._execute_via_safe(redeem_data)
        else:
            return self._execute_direct(redeem_data)

    def _execute_direct(self, data: str) -> RedemptionResult:
        """Execute redeemPositions directly from EOA."""
        try:
            # Build transaction
            tx: TxParams = {
                "from": self.eoa_address,
                "to": CTF_ADDRESS,
                "data": data,
                "gas": 200000,
                "gasPrice": self.w3.eth.gas_price,
                "nonce": self.w3.eth.get_transaction_count(self.eoa_address),
                "chainId": 137,  # Polygon
            }

            # Sign and send
            signed = self.account.sign_transaction(tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)

            logger.info(f"Transaction sent: {tx_hash.hex()}")

            # Wait for confirmation
            receipt: TxReceipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

            if receipt["status"] == 1:
                return RedemptionResult(
                    success=True,
                    tx_hash=tx_hash.hex(),
                    gas_used=receipt["gasUsed"],
                )
            else:
                return RedemptionResult(
                    success=False,
                    tx_hash=tx_hash.hex(),
                    error="Transaction reverted",
                )

        except Exception as e:
            logger.error(f"Direct execution failed: {e}")
            return RedemptionResult(success=False, error=str(e))

    def _execute_via_safe(self, data: str) -> RedemptionResult:
        """Execute redeemPositions through Safe wallet."""
        try:
            if not self.safe_contract:
                return RedemptionResult(success=False, error="Safe contract not initialized")

            # Get Safe nonce
            nonce = self.safe_contract.functions.nonce().call()

            # Parameters for execTransaction
            to = CTF_ADDRESS
            value = 0
            operation = 0  # CALL (not DELEGATECALL)
            safe_tx_gas = 0  # Let Safe estimate
            base_gas = 0
            gas_price = 0  # No refund
            gas_token = NULL_ADDRESS
            refund_receiver = NULL_ADDRESS

            # Get transaction hash for signing
            tx_hash = self.safe_contract.functions.getTransactionHash(
                to,
                value,
                Web3.to_bytes(hexstr=data),
                operation,
                safe_tx_gas,
                base_gas,
                gas_price,
                gas_token,
                refund_receiver,
                nonce,
            ).call()

            # Sign the hash
            # For 1-of-1 Safe, we need a single signature
            # Signature format: {bytes32 r}{bytes32 s}{uint8 v}
            signed = Account.unsafe_sign_hash(tx_hash, self._private_key)

            # Pack signature: r (32) + s (32) + v (1)
            signature = (
                signed.r.to_bytes(32, "big") + signed.s.to_bytes(32, "big") + bytes([signed.v])
            )

            # Build execTransaction call
            exec_data = self.safe_contract.encode_abi(
                "execTransaction",
                args=[
                    to,
                    value,
                    Web3.to_bytes(hexstr=data),
                    operation,
                    safe_tx_gas,
                    base_gas,
                    gas_price,
                    gas_token,
                    refund_receiver,
                    signature,
                ],
            )
            tx: TxParams = {
                "from": self.eoa_address,
                "to": self.safe_address,
                "data": exec_data,
                "gas": 300000,
                "gasPrice": self.w3.eth.gas_price,
                "nonce": self.w3.eth.get_transaction_count(self.eoa_address),
                "chainId": 137,
            }

            # Sign and send
            signed_tx = self.account.sign_transaction(tx)
            tx_hash_bytes = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)

            logger.info(f"Safe transaction sent: {tx_hash_bytes.hex()}")

            # Wait for confirmation
            receipt: TxReceipt = self.w3.eth.wait_for_transaction_receipt(
                tx_hash_bytes, timeout=120
            )

            if receipt["status"] == 1:
                return RedemptionResult(
                    success=True,
                    tx_hash=tx_hash_bytes.hex(),
                    gas_used=receipt["gasUsed"],
                )
            else:
                return RedemptionResult(
                    success=False,
                    tx_hash=tx_hash_bytes.hex(),
                    error="Safe transaction reverted",
                )

        except Exception as e:
            logger.error(f"Safe execution failed: {e}")
            return RedemptionResult(success=False, error=str(e))

    def _build_hmac_signature(
        self, secret: str, timestamp: str, method: str, request_path: str, body: str | None = None
    ) -> str:
        """Create HMAC signature for Polymarket API authentication."""
        import base64
        import hashlib
        import hmac as hmac_lib

        base64_secret = base64.urlsafe_b64decode(secret)
        message = str(timestamp) + str(method) + str(request_path)
        if body:
            message += body

        h = hmac_lib.new(base64_secret, bytes(message, "utf-8"), hashlib.sha256)
        return base64.urlsafe_b64encode(h.digest()).decode("utf-8")

    def _execute_via_relayer(self, condition_id: str) -> RedemptionResult:
        """Execute redemption via Polymarket's gasless relayer.

        Requires Builder API credentials in settings.
        """
        import json
        import time

        settings = get_settings()

        # Check for Builder credentials
        api_key = settings.polymarket_api_key.get_secret_value()
        api_secret = settings.polymarket_api_secret.get_secret_value()
        api_passphrase = settings.polymarket_api_passphrase.get_secret_value()

        if not all([api_key, api_secret, api_passphrase]):
            return RedemptionResult(
                success=False,
                error="Builder API credentials required for gasless redemption",
            )

        if not self.safe_address:
            return RedemptionResult(
                success=False,
                error="Safe/proxy address required for gasless redemption",
            )

        try:
            # Ensure condition_id format
            if not condition_id.startswith("0x"):
                condition_id = "0x" + condition_id
            condition_bytes = Web3.to_bytes(hexstr=condition_id)

            # Encode redeemPositions call
            redeem_data = self.ctf_contract.encode_abi(
                "redeemPositions",
                args=[
                    USDC_ADDRESS,
                    bytes(32),  # parentCollectionId
                    condition_bytes,
                    [1, 2],  # indexSets
                ],
            )

            # Step 1: Get nonce from relayer
            # Polymarket accounts are typically Safe wallets
            signer_type = "SAFE"
            timestamp = str(int(time.time()))
            nonce_path = "/nonce"
            nonce_params = f"?address={self.safe_address}&type={signer_type}"

            nonce_signature = self._build_hmac_signature(
                api_secret, timestamp, "GET", nonce_path + nonce_params
            )

            nonce_headers = {
                "POLY_BUILDER_API_KEY": api_key,
                "POLY_BUILDER_PASSPHRASE": api_passphrase,
                "POLY_BUILDER_SIGNATURE": nonce_signature,
                "POLY_BUILDER_TIMESTAMP": timestamp,
            }

            logger.info(f"Getting nonce from relayer (type={signer_type})...")
            nonce_resp = httpx.get(
                f"{RELAYER_URL}{nonce_path}",
                params={"address": self.safe_address, "type": signer_type},
                headers=nonce_headers,
                timeout=30.0,
            )

            if nonce_resp.status_code != 200:
                logger.error(f"Nonce response: {nonce_resp.status_code} - {nonce_resp.text}")
                return RedemptionResult(
                    success=False,
                    error=f"Failed to get nonce: {nonce_resp.status_code} - {nonce_resp.text}",
                )

            nonce_data = nonce_resp.json()
            relayer_nonce = int(nonce_data.get("nonce", 0))
            logger.info(f"Got relayer nonce: {relayer_nonce}")

            # Get the actual Safe contract nonce for signing
            safe_nonce = self.safe_contract.functions.nonce().call()
            logger.info(f"Got Safe contract nonce: {safe_nonce}")

            # Step 2: Build and sign Safe transaction
            # Safe transaction parameters
            safe_tx_gas = 0
            base_gas = 0
            gas_price = 0
            gas_token = NULL_ADDRESS
            refund_receiver = NULL_ADDRESS
            operation = 0  # Call

            # Get Safe transaction hash for signing
            if not self.safe_contract:
                return RedemptionResult(
                    success=False,
                    error="Safe contract not initialized",
                )

            safe_tx_hash = self.safe_contract.functions.getTransactionHash(
                CTF_ADDRESS,  # to
                0,  # value
                Web3.to_bytes(hexstr=redeem_data),  # data
                operation,
                safe_tx_gas,
                base_gas,
                gas_price,
                gas_token,
                refund_receiver,
                safe_nonce,  # Use Safe contract nonce for signing
            ).call()

            # Sign the Safe transaction hash using eth_sign method
            # This adds the "\x19Ethereum Signed Message:\n32" prefix
            from eth_account.messages import encode_defunct

            message = encode_defunct(primitive=safe_tx_hash)
            signed = Account.sign_message(message, self._private_key)

            # Pack signature: r + s + v
            signature = (
                signed.r.to_bytes(32, "big") + signed.s.to_bytes(32, "big") + bytes([signed.v])
            )
            signature_hex = "0x" + signature.hex()

            # Adjust v-value for Safe relayer eth_sign type
            # Safe uses signature type 1f (31) for eth_sign with v=27
            # and type 20 (32) for eth_sign with v=28
            v_hex = signature_hex[-2:]
            if v_hex in ("1b",):  # v=27
                signature_hex = signature_hex[:-2] + "1f"
            elif v_hex in ("1c",):  # v=28
                signature_hex = signature_hex[:-2] + "20"

            # Build relayer payload
            # signatureParams - all values must be strings
            signature_params = {
                "baseGas": str(base_gas),
                "gasPrice": str(gas_price),
                "gasToken": gas_token,
                "operation": str(operation),  # Must be string "0"
                "refundReceiver": refund_receiver,
                "safeTxnGas": str(safe_tx_gas),
            }

            tx_request = {
                "type": signer_type,  # "SAFE"
                "proxyWallet": self.safe_address,
                "to": CTF_ADDRESS,
                "data": redeem_data,
                "from": self.eoa_address,
                "nonce": str(safe_nonce),  # Use Safe nonce in payload too
                "signature": signature_hex,
                "signatureParams": signature_params,
                "metadata": "redeem",
            }

            body = json.dumps(tx_request)
            timestamp = str(int(time.time()))
            submit_path = "/submit"

            # Create HMAC signature for submit
            submit_signature = self._build_hmac_signature(
                api_secret, timestamp, "POST", submit_path, body
            )

            headers = {
                "Content-Type": "application/json",
                "POLY_BUILDER_API_KEY": api_key,
                "POLY_BUILDER_PASSPHRASE": api_passphrase,
                "POLY_BUILDER_SIGNATURE": submit_signature,
                "POLY_BUILDER_TIMESTAMP": timestamp,
            }

            logger.info(f"Submitting to relayer: {RELAYER_URL}{submit_path}")
            logger.debug(f"Payload: {body}")

            # Submit to relayer
            resp = httpx.post(
                f"{RELAYER_URL}{submit_path}",
                content=body,
                headers=headers,
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(f"Relayer response: {resp.status_code} - {resp.text}")
                return RedemptionResult(
                    success=False,
                    error=f"Relayer error: {resp.status_code} - {resp.text}",
                )

            result = resp.json()
            tx_hash = result.get("transactionHash") or result.get("hash") or result.get("txHash")

            if tx_hash:
                logger.info(f"Relayer transaction submitted: {tx_hash}")
                return RedemptionResult(
                    success=True,
                    tx_hash=tx_hash,
                )
            else:
                # Some relayers return success without immediate tx hash
                logger.info(f"Relayer response: {result}")
                return RedemptionResult(
                    success=True,
                    error=f"Submitted, response: {result}",
                )

        except Exception as e:
            logger.error(f"Relayer execution failed: {e}")
            return RedemptionResult(success=False, error=str(e))

    def redeem_position_gasless(self, condition_id: str) -> RedemptionResult:
        """Redeem a winning position using gasless relayer.

        Args:
            condition_id: The market's condition ID

        Returns:
            RedemptionResult with transaction details
        """
        # Check resolution first
        resolution = self.check_resolution(condition_id)
        if not resolution.is_resolved:
            return RedemptionResult(
                success=False,
                error=f"Market {condition_id[:10]}... is not resolved yet",
            )

        logger.info(
            f"Market resolved - winning outcome: "
            f"{'YES' if resolution.winning_outcome == 0 else 'NO'}"
        )

        return self._execute_via_relayer(condition_id)

    def has_builder_credentials(self) -> bool:
        """Check if Builder API credentials are configured for gasless transactions."""
        settings = get_settings()
        return bool(
            settings.polymarket_api_key.get_secret_value()
            and settings.polymarket_api_secret.get_secret_value()
            and settings.polymarket_api_passphrase.get_secret_value()
        )

    def redeem_all_resolved(
        self,
        condition_ids: list[str],
        dry_run: bool = True,
        use_gasless: bool | None = None,
    ) -> dict[str, RedemptionResult]:
        """Redeem all resolved positions from a list.

        Args:
            condition_ids: List of market condition IDs
            dry_run: If True, simulate without executing
            use_gasless: If True, use relayer for gasless tx. Auto-detects if None.

        Returns:
            Dict mapping condition_id to RedemptionResult
        """
        results: dict[str, RedemptionResult] = {}

        # Auto-detect gasless mode based on credentials
        if use_gasless is None:
            use_gasless = self.has_builder_credentials()

        if use_gasless:
            logger.info("Using gasless relayer for redemptions")
        else:
            logger.info("Using direct transaction (requires POL for gas)")

        for condition_id in condition_ids:
            logger.info(f"Processing {condition_id[:10]}...")

            # Check resolution
            resolution = self.check_resolution(condition_id)

            if not resolution.is_resolved:
                logger.info("  Skipping - not resolved")
                results[condition_id] = RedemptionResult(
                    success=False,
                    error="Not resolved",
                )
                continue

            # Attempt redemption
            if dry_run:
                result = self.redeem_position(condition_id, dry_run=True)
            elif use_gasless:
                result = self.redeem_position_gasless(condition_id)
            else:
                result = self.redeem_position(condition_id, dry_run=False)

            results[condition_id] = result

            if result.success:
                logger.info(f"  Redeemed successfully: {result.tx_hash}")
            else:
                logger.warning(f"  Failed: {result.error}")

        return results
