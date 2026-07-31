use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::{
    model::{FeatureSnapshot, Side, STRATEGY_VERSION},
    position::{ExitReason, Position, PositionStage},
};

const SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackedPosition {
    pub id: u64,
    pub symbol: String,
    pub strategy_version: String,
    pub market_regime: String,
    pub opened_at: i64,
    pub initial_margin: f64,
    pub position: Position,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PortfolioSnapshot {
    pub schema_version: u32,
    pub initial_balance: f64,
    pub balance: f64,
    pub realized_net_pnl: f64,
    pub entries_paused: bool,
    pub pause_reason: Option<String>,
    pub next_position_id: u64,
    pub positions: Vec<TrackedPosition>,
}

impl PortfolioSnapshot {
    pub fn fresh(initial_balance: f64) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            initial_balance,
            balance: initial_balance,
            realized_net_pnl: 0.0,
            entries_paused: false,
            pause_reason: None,
            next_position_id: 1,
            positions: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LedgerEvent {
    pub event_key: String,
    pub position_id: u64,
    pub symbol: String,
    pub side: Side,
    pub stage: PositionStage,
    pub event_type: String,
    pub quantity: f64,
    pub price: f64,
    pub net_pnl: f64,
    pub fee: f64,
    pub funding: f64,
    pub exit_reason: Option<ExitReason>,
    pub payload_json: String,
    pub created_at: i64,
}

#[derive(Clone, Debug)]
pub struct SignalDecision {
    pub decision_key: String,
    pub position_id: Option<u64>,
    pub symbol: String,
    pub side: Option<Side>,
    pub score: f64,
    pub confidence: f64,
    pub accepted: bool,
    pub reject_reason: Option<String>,
    pub features: FeatureSnapshot,
    pub observed_at: i64,
}

pub struct SqliteStore {
    connection: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path.as_ref())
            .with_context(|| format!("SQLite açılamadı: {}", path.as_ref().display()))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("SQLite busy_timeout ayarlanamadı")?;
        connection
            .execute_batch(
                "
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                PRAGMA foreign_keys=ON;
                PRAGMA temp_store=MEMORY;

                CREATE TABLE IF NOT EXISTS state_snapshot (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    schema_version INTEGER NOT NULL,
                    strategy_version TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execution_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT NOT NULL UNIQUE,
                    position_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),
                    stage TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL NOT NULL,
                    net_pnl REAL NOT NULL,
                    fee REAL NOT NULL,
                    funding REAL NOT NULL,
                    exit_reason TEXT,
                    payload_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_execution_position
                    ON execution_ledger(position_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_execution_symbol
                    ON execution_ledger(symbol, created_at);

                CREATE TABLE IF NOT EXISTS signal_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_key TEXT NOT NULL UNIQUE,
                    position_id INTEGER,
                    symbol TEXT NOT NULL,
                    side TEXT,
                    score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    accepted INTEGER NOT NULL CHECK (accepted IN (0, 1)),
                    reject_reason TEXT,
                    features_json TEXT NOT NULL,
                    observed_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_signal_reason
                    ON signal_decisions(accepted, reject_reason, observed_at);
                CREATE INDEX IF NOT EXISTS idx_signal_side
                    ON signal_decisions(side, accepted, observed_at);
                CREATE INDEX IF NOT EXISTS idx_signal_position
                    ON signal_decisions(position_id);
                ",
            )
            .context("SQLite şeması hazırlanamadı")?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn load_or_create(&self, initial_balance: f64, now: i64) -> Result<PortfolioSnapshot> {
        let connection = self.lock()?;
        let stored = connection.query_row(
            "SELECT schema_version, payload_json FROM state_snapshot WHERE id = 1",
            [],
            |row| Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?)),
        );
        match stored {
            Ok((schema_version, payload)) => {
                if schema_version != SCHEMA_VERSION {
                    anyhow::bail!(
                        "Desteklenmeyen SQLite şema sürümü: {schema_version}; beklenen: {SCHEMA_VERSION}"
                    );
                }
                let snapshot: PortfolioSnapshot =
                    serde_json::from_str(&payload).context("state_snapshot JSON bozuk")?;
                validate_snapshot(&snapshot)?;
                Ok(snapshot)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                drop(connection);
                let snapshot = PortfolioSnapshot::fresh(initial_balance);
                self.persist_atomic(&snapshot, &[], &[], now)?;
                Ok(snapshot)
            }
            Err(error) => Err(error).context("state_snapshot okunamadı"),
        }
    }

    /// The snapshot and its execution events commit together. A crash can
    /// therefore never expose a new balance without the matching fill ledger.
    pub fn persist_atomic(
        &self,
        snapshot: &PortfolioSnapshot,
        ledger_events: &[LedgerEvent],
        signal_decisions: &[SignalDecision],
        now: i64,
    ) -> Result<()> {
        validate_snapshot(snapshot)?;
        let payload = serde_json::to_string(snapshot).context("AppState JSON üretilemedi")?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("SQLite transaction başlatılamadı")?;

        transaction
            .execute(
                "
                INSERT INTO state_snapshot (
                    id, schema_version, strategy_version, payload_json, updated_at
                ) VALUES (1, ?1, ?2, ?3, ?4)
                ON CONFLICT(id) DO UPDATE SET
                    schema_version = excluded.schema_version,
                    strategy_version = excluded.strategy_version,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                ",
                params![SCHEMA_VERSION, STRATEGY_VERSION, payload, now],
            )
            .context("state_snapshot yazılamadı")?;

        for event in ledger_events {
            let position_id = sqlite_integer(event.position_id).with_context(|| {
                format!(
                    "ledger position_id SQLite sınırını aşıyor: {}",
                    event.position_id
                )
            })?;
            transaction
                .execute(
                    "
                    INSERT OR IGNORE INTO execution_ledger (
                        event_key, position_id, symbol, side, stage, event_type,
                        quantity, price, net_pnl, fee, funding, exit_reason,
                        payload_json, created_at
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
                    )
                    ",
                    params![
                        event.event_key,
                        position_id,
                        event.symbol,
                        side_name(event.side),
                        stage_name(event.stage),
                        event.event_type,
                        event.quantity,
                        event.price,
                        event.net_pnl,
                        event.fee,
                        event.funding,
                        event.exit_reason.map(exit_reason_name),
                        event.payload_json,
                        event.created_at,
                    ],
                )
                .with_context(|| format!("ledger olayı yazılamadı: {}", event.event_key))?;
        }

        for decision in signal_decisions {
            let features_json =
                serde_json::to_string(&decision.features).context("feature JSON üretilemedi")?;
            let position_id = decision
                .position_id
                .map(sqlite_integer)
                .transpose()
                .with_context(|| {
                    format!(
                        "signal position_id SQLite sınırını aşıyor: {:?}",
                        decision.position_id
                    )
                })?;
            transaction
                .execute(
                    "
                    INSERT OR IGNORE INTO signal_decisions (
                        decision_key, position_id, symbol, side, score, confidence,
                        accepted, reject_reason, features_json, observed_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                    ",
                    params![
                        decision.decision_key,
                        position_id,
                        decision.symbol,
                        decision.side.map(side_name),
                        decision.score,
                        decision.confidence,
                        if decision.accepted { 1i64 } else { 0i64 },
                        decision.reject_reason,
                        features_json,
                        decision.observed_at,
                    ],
                )
                .with_context(|| {
                    format!("signal decision yazılamadı: {}", decision.decision_key)
                })?;
        }

        transaction.commit().context("SQLite commit başarısız")
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.lock()?
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .context("SQLite WAL checkpoint başarısız")
    }

    /// Keeps accepted training labels indefinitely while bounding noisy gate
    /// rejection telemetry. Execution and position ledgers are never pruned.
    pub fn maintain(&self, rejected_before: i64) -> Result<usize> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("SQLite bakım transaction başlatılamadı")?;
        let deleted = transaction
            .execute(
                "DELETE FROM signal_decisions WHERE accepted = 0 AND observed_at < ?1",
                params![rejected_before],
            )
            .context("Eski sinyal retleri temizlenemedi")?;
        transaction.commit().context("SQLite bakım commit başarısız")?;
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .context("SQLite bakım checkpoint başarısız")?;
        Ok(deleted)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite mutex poisoned"))
    }
}

fn validate_snapshot(snapshot: &PortfolioSnapshot) -> Result<()> {
    if snapshot.schema_version != SCHEMA_VERSION {
        anyhow::bail!("PortfolioSnapshot schema_version geçersiz");
    }
    if !snapshot.initial_balance.is_finite()
        || !snapshot.balance.is_finite()
        || !snapshot.realized_net_pnl.is_finite()
        || snapshot.initial_balance <= 0.0
        || snapshot.balance < 0.0
        || snapshot.next_position_id == 0
    {
        anyhow::bail!("PortfolioSnapshot sayısal alanları geçersiz");
    }
    for tracked in &snapshot.positions {
        if tracked.id == 0
            || tracked.symbol.is_empty()
            || !tracked.initial_margin.is_finite()
            || tracked.initial_margin <= 0.0
            || tracked.position.remaining_quantity <= 0.0
            || tracked.position.stage == PositionStage::Closed
        {
            anyhow::bail!("Geçersiz aktif pozisyon kaydı: {}", tracked.id);
        }
    }
    Ok(())
}

fn sqlite_integer(value: u64) -> Result<i64> {
    i64::try_from(value).context("u64 değeri SQLite INTEGER alanına sığmıyor")
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Long => "LONG",
        Side::Short => "SHORT",
    }
}

fn stage_name(stage: PositionStage) -> &'static str {
    match stage {
        PositionStage::BeforeTp1 => "BEFORE_TP1",
        PositionStage::AfterTp1 => "AFTER_TP1",
        PositionStage::Runner => "RUNNER",
        PositionStage::Closed => "CLOSED",
    }
}

fn exit_reason_name(reason: ExitReason) -> &'static str {
    match reason {
        ExitReason::InitialStop => "INITIAL_STOP",
        ExitReason::PreTp1Ratchet => "PRE_TP1_RATCHET",
        ExitReason::Tp1Stop => "TP1_STOP",
        ExitReason::RunnerTrail => "RUNNER_TRAIL",
        ExitReason::TrendInvalidation => "TREND_INVALIDATION",
        ExitReason::FundingExit => "FUNDING_EXIT",
        ExitReason::EmergencyExit => "EMERGENCY_EXIT",
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn snapshot_and_ledger_commit_and_reload_together() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("quant-v4-{unique}.db"));
        let store = SqliteStore::open(&path).unwrap();
        let mut snapshot = store.load_or_create(10_000.0, 1).unwrap();
        snapshot.balance = 10_012.5;
        snapshot.realized_net_pnl = 12.5;
        store.persist_atomic(&snapshot, &[], &[], 2).unwrap();
        let restored = store.load_or_create(1.0, 3).unwrap();
        assert_eq!(restored.balance, 10_012.5);
        assert_eq!(restored.realized_net_pnl, 12.5);
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn sqlite_ids_reject_unsigned_overflow() {
        assert_eq!(sqlite_integer(i64::MAX as u64).unwrap(), i64::MAX);
        assert!(sqlite_integer(u64::MAX).is_err());
    }
}
