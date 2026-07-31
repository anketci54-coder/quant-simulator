use crate::{cost::CostModel, model::Side};
#[cfg(not(v4_standalone_verify))]
use serde::{Deserialize, Serialize};

const EPSILON: f64 = 1e-9;

#[cfg_attr(not(v4_standalone_verify), derive(Deserialize, Serialize))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionStage {
    BeforeTp1,
    AfterTp1,
    Runner,
    Closed,
}

#[cfg_attr(not(v4_standalone_verify), derive(Deserialize, Serialize))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitReason {
    InitialStop,
    PreTp1Ratchet,
    Tp1Stop,
    RunnerTrail,
    TrendInvalidation,
    FundingExit,
    EmergencyExit,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PositionEvent {
    StopMoved {
        old: f64,
        new: f64,
        stage: PositionStage,
    },
    PartialExit {
        fraction_of_original: f64,
        quantity: f64,
        exit_fill: f64,
        entry_fee_allocated: f64,
        exit_fee: f64,
        funding: f64,
        net_pnl: f64,
        stage: PositionStage,
    },
    Closed {
        quantity: f64,
        exit_fill: f64,
        entry_fee_allocated: f64,
        exit_fee: f64,
        funding: f64,
        net_pnl: f64,
        reason: ExitReason,
    },
}

#[cfg_attr(not(v4_standalone_verify), derive(Deserialize, Serialize))]
#[derive(Clone, Copy, Debug)]
pub struct PositionPolicy {
    pub tp1_fraction: f64,
    pub tp2_fraction: f64,
    pub runner_fraction: f64,
    pub pre_tp1_ratchet_start: f64,
    pub tp1_lock_r: f64,
    pub runner_atr_multiple: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct PositionQuote {
    pub bid: f64,
    pub ask: f64,
    pub atr: f64,
    pub structure_stop: Option<f64>,
    pub step_size: f64,
}

impl Default for PositionPolicy {
    fn default() -> Self {
        Self {
            tp1_fraction: 0.40,
            tp2_fraction: 0.40,
            runner_fraction: 0.20,
            pre_tp1_ratchet_start: 0.65,
            tp1_lock_r: 0.10,
            runner_atr_multiple: 2.2,
        }
    }
}

impl PositionPolicy {
    pub fn validate(self) -> bool {
        let total = self.tp1_fraction + self.tp2_fraction + self.runner_fraction;
        self.tp1_fraction > 0.0
            && self.tp2_fraction > 0.0
            && self.runner_fraction > 0.0
            && (total - 1.0).abs() <= EPSILON
            && (0.0..1.0).contains(&self.pre_tp1_ratchet_start)
            && (0.0..=1.0).contains(&self.tp1_lock_r)
            && self.runner_atr_multiple > 0.0
    }
}

#[cfg_attr(not(v4_standalone_verify), derive(Deserialize, Serialize))]
#[derive(Clone, Debug)]
pub struct Position {
    pub side: Side,
    pub entry_fill: f64,
    pub original_quantity: f64,
    pub remaining_quantity: f64,
    pub initial_stop: f64,
    pub stop: f64,
    pub tp1: f64,
    pub tp2: f64,
    pub stage: PositionStage,
    pub favorable_extreme: f64,
    pub last_exit_fill: f64,
    pub funding_cost: f64,
    pub realized_net_pnl: f64,
    pub entry_fee_remaining: f64,
}

impl Position {
    pub fn new(
        side: Side,
        entry_fill: f64,
        quantity: f64,
        initial_stop: f64,
        tp1: f64,
        tp2: f64,
        costs: CostModel,
    ) -> Option<Self> {
        let correctly_ordered = match side {
            Side::Long => initial_stop < entry_fill && entry_fill < tp1 && tp1 < tp2,
            Side::Short => initial_stop > entry_fill && entry_fill > tp1 && tp1 > tp2,
        };
        if entry_fill <= 0.0 || quantity <= 0.0 || !correctly_ordered || !costs.validate() {
            return None;
        }
        Some(Self {
            side,
            entry_fill,
            original_quantity: quantity,
            remaining_quantity: quantity,
            initial_stop,
            stop: initial_stop,
            tp1,
            tp2,
            stage: PositionStage::BeforeTp1,
            favorable_extreme: entry_fill,
            last_exit_fill: entry_fill,
            funding_cost: 0.0,
            realized_net_pnl: 0.0,
            entry_fee_remaining: costs.entry_fee(entry_fill, quantity),
        })
    }

    pub fn risk_per_unit(&self) -> f64 {
        (self.entry_fill - self.initial_stop).abs()
    }

    pub fn add_funding_cost(&mut self, funding_cost: f64) {
        if funding_cost.is_finite() {
            self.funding_cost += funding_cost;
        }
    }

    /// Advances one position from an executable bid/ask quote. Partial exits
    /// are 40% + 40% of the original quantity; the last 20% has no fixed TP.
    pub fn on_quote(
        &mut self,
        quote: PositionQuote,
        costs: CostModel,
        policy: PositionPolicy,
    ) -> Vec<PositionEvent> {
        let mut events = Vec::new();
        if self.stage == PositionStage::Closed
            || quote.bid <= 0.0
            || quote.ask < quote.bid
            || quote.atr <= 0.0
            || quote.step_size <= 0.0
            || !policy.validate()
        {
            return events;
        }

        let executable = self.side.favorable_price(quote.bid, quote.ask);
        if let Some(exit_fill) = costs.estimated_exit_fill(self.side, quote.bid, quote.ask) {
            self.last_exit_fill = exit_fill;
        }
        self.favorable_extreme = match self.side {
            Side::Long => self.favorable_extreme.max(executable),
            Side::Short => self.favorable_extreme.min(executable),
        };

        self.apply_protective_stop(costs, policy, quote.atr, quote.structure_stop, &mut events);

        if self.side.stop_is_hit(quote.bid, quote.ask, self.stop) {
            let reason = match self.stage {
                PositionStage::BeforeTp1 if self.stop == self.initial_stop => {
                    ExitReason::InitialStop
                }
                PositionStage::BeforeTp1 => ExitReason::PreTp1Ratchet,
                PositionStage::AfterTp1 => ExitReason::Tp1Stop,
                PositionStage::Runner => ExitReason::RunnerTrail,
                PositionStage::Closed => return events,
            };
            self.close_remaining(quote.bid, quote.ask, costs, reason, &mut events);
            return events;
        }

        if self.stage == PositionStage::BeforeTp1 && self.target_reached(executable, self.tp1) {
            self.partial_exit(
                policy.tp1_fraction,
                quote,
                costs,
                PositionStage::AfterTp1,
                &mut events,
            );
            self.raise_stop_after_tp1(costs, policy, &mut events);
        }

        if self.stage == PositionStage::AfterTp1 && self.target_reached(executable, self.tp2) {
            self.partial_exit(
                policy.tp2_fraction,
                quote,
                costs,
                PositionStage::Runner,
                &mut events,
            );
            self.raise_stop_to_tp1(&mut events);
            self.apply_runner_trail(quote.atr, quote.structure_stop, policy, &mut events);
        }

        events
    }

    pub fn force_close(
        &mut self,
        bid: f64,
        ask: f64,
        costs: CostModel,
        reason: ExitReason,
    ) -> Vec<PositionEvent> {
        let mut events = Vec::new();
        if self.stage != PositionStage::Closed && bid > 0.0 && ask >= bid {
            if let Some(exit_fill) = costs.estimated_exit_fill(self.side, bid, ask) {
                self.last_exit_fill = exit_fill;
            }
            self.close_remaining(bid, ask, costs, reason, &mut events);
        }
        events
    }

    pub fn unrealized_net_pnl(&self, costs: CostModel) -> f64 {
        if self.stage == PositionStage::Closed || self.remaining_quantity <= 0.0 {
            return 0.0;
        }
        costs.net_pnl(
            self.side,
            self.entry_fill,
            self.last_exit_fill,
            self.remaining_quantity,
            self.entry_fee_remaining,
            self.funding_cost,
        )
    }

    fn target_reached(&self, price: f64, target: f64) -> bool {
        match self.side {
            Side::Long => price >= target,
            Side::Short => price <= target,
        }
    }

    fn apply_protective_stop(
        &mut self,
        costs: CostModel,
        policy: PositionPolicy,
        atr: f64,
        structure_stop: Option<f64>,
        events: &mut Vec<PositionEvent>,
    ) {
        match self.stage {
            PositionStage::BeforeTp1 => {
                let full_distance = (self.tp1 - self.entry_fill).abs();
                let progress = if full_distance > 0.0 {
                    ((self.favorable_extreme - self.entry_fill) * self.side.direction()
                        / full_distance)
                        .clamp(0.0, 1.0)
                } else {
                    0.0
                };
                if progress >= policy.pre_tp1_ratchet_start {
                    let t = (progress - policy.pre_tp1_ratchet_start)
                        / (1.0 - policy.pre_tp1_ratchet_start);
                    if let Some(break_even) = costs.break_even_trigger(
                        self.side,
                        self.entry_fill,
                        self.remaining_quantity,
                        self.funding_cost,
                    ) {
                        let ratchet = self.initial_stop + (break_even - self.initial_stop) * t;
                        self.move_stop_monotonically(ratchet, events);
                    }
                }
            }
            PositionStage::AfterTp1 => self.raise_stop_after_tp1(costs, policy, events),
            PositionStage::Runner => self.apply_runner_trail(atr, structure_stop, policy, events),
            PositionStage::Closed => {}
        }
    }

    fn raise_stop_after_tp1(
        &mut self,
        costs: CostModel,
        policy: PositionPolicy,
        events: &mut Vec<PositionEvent>,
    ) {
        let r_lock =
            self.entry_fill + self.side.direction() * self.risk_per_unit() * policy.tp1_lock_r;
        if let Some(break_even) = costs.break_even_trigger(
            self.side,
            self.entry_fill,
            self.remaining_quantity,
            self.funding_cost,
        ) {
            let candidate = match self.side {
                Side::Long => break_even.max(r_lock),
                Side::Short => break_even.min(r_lock),
            };
            self.move_stop_monotonically(candidate, events);
        }
    }

    fn raise_stop_to_tp1(&mut self, events: &mut Vec<PositionEvent>) {
        self.move_stop_monotonically(self.tp1, events);
    }

    fn apply_runner_trail(
        &mut self,
        atr: f64,
        structure_stop: Option<f64>,
        policy: PositionPolicy,
        events: &mut Vec<PositionEvent>,
    ) {
        let chandelier =
            self.favorable_extreme - self.side.direction() * atr * policy.runner_atr_multiple;
        let mut candidate = match self.side {
            Side::Long => chandelier.max(self.tp1),
            Side::Short => chandelier.min(self.tp1),
        };
        if let Some(structure) = structure_stop.filter(|price| price.is_finite() && *price > 0.0) {
            candidate = match self.side {
                Side::Long => candidate.max(structure),
                Side::Short => candidate.min(structure),
            };
        }
        self.move_stop_monotonically(candidate, events);
    }

    fn move_stop_monotonically(&mut self, candidate: f64, events: &mut Vec<PositionEvent>) {
        if !candidate.is_finite() || candidate <= 0.0 {
            return;
        }
        let improves = match self.side {
            Side::Long => candidate > self.stop + EPSILON,
            Side::Short => candidate < self.stop - EPSILON,
        };
        if improves {
            let old = self.stop;
            self.stop = candidate;
            events.push(PositionEvent::StopMoved {
                old,
                new: candidate,
                stage: self.stage,
            });
        }
    }

    fn partial_exit(
        &mut self,
        fraction_of_original: f64,
        quote: PositionQuote,
        costs: CostModel,
        next_stage: PositionStage,
        events: &mut Vec<PositionEvent>,
    ) {
        let desired = self.original_quantity * fraction_of_original;
        let quantity = round_down(desired, quote.step_size).min(self.remaining_quantity);
        if quantity <= 0.0 {
            return;
        }
        let Some(exit_fill) = costs.estimated_exit_fill(self.side, quote.bid, quote.ask) else {
            return;
        };
        let entry_fee = self.allocated_entry_fee(quantity);
        let funding = self.allocated_funding(quantity);
        let exit_fee = costs.exit_fee(exit_fill, quantity);
        let net_pnl = costs.net_pnl(
            self.side,
            self.entry_fill,
            exit_fill,
            quantity,
            entry_fee,
            funding,
        );
        self.remaining_quantity = (self.remaining_quantity - quantity).max(0.0);
        self.realized_net_pnl += net_pnl;
        self.stage = next_stage;
        events.push(PositionEvent::PartialExit {
            fraction_of_original,
            quantity,
            exit_fill,
            entry_fee_allocated: entry_fee,
            exit_fee,
            funding,
            net_pnl,
            stage: next_stage,
        });
    }

    fn close_remaining(
        &mut self,
        bid: f64,
        ask: f64,
        costs: CostModel,
        reason: ExitReason,
        events: &mut Vec<PositionEvent>,
    ) {
        let quantity = self.remaining_quantity;
        let Some(exit_fill) = costs.estimated_exit_fill(self.side, bid, ask) else {
            return;
        };
        let entry_fee = self.entry_fee_remaining;
        let funding = self.funding_cost;
        let exit_fee = costs.exit_fee(exit_fill, quantity);
        let net_pnl = costs.net_pnl(
            self.side,
            self.entry_fill,
            exit_fill,
            quantity,
            entry_fee,
            funding,
        );
        self.entry_fee_remaining = 0.0;
        self.funding_cost = 0.0;
        self.remaining_quantity = 0.0;
        self.realized_net_pnl += net_pnl;
        self.stage = PositionStage::Closed;
        events.push(PositionEvent::Closed {
            quantity,
            exit_fill,
            entry_fee_allocated: entry_fee,
            exit_fee,
            funding,
            net_pnl,
            reason,
        });
    }

    fn allocated_entry_fee(&mut self, quantity: f64) -> f64 {
        if self.remaining_quantity <= 0.0 {
            return 0.0;
        }
        let allocated = self.entry_fee_remaining * quantity / self.remaining_quantity;
        self.entry_fee_remaining = (self.entry_fee_remaining - allocated).max(0.0);
        allocated
    }

    fn allocated_funding(&mut self, quantity: f64) -> f64 {
        if self.remaining_quantity <= 0.0 {
            return 0.0;
        }
        let allocated = self.funding_cost * quantity / self.remaining_quantity;
        self.funding_cost -= allocated;
        allocated
    }
}

fn round_down(value: f64, step_size: f64) -> f64 {
    ((value / step_size) + EPSILON).floor() * step_size
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote(bid: f64, ask: f64, atr: f64, structure_stop: Option<f64>) -> PositionQuote {
        PositionQuote {
            bid,
            ask,
            atr,
            structure_stop,
            step_size: 0.1,
        }
    }

    fn position(side: Side) -> Position {
        match side {
            Side::Long => {
                Position::new(side, 100.0, 10.0, 98.0, 102.0, 104.0, CostModel::default()).unwrap()
            }
            Side::Short => {
                Position::new(side, 100.0, 10.0, 102.0, 98.0, 96.0, CostModel::default()).unwrap()
            }
        }
    }

    #[test]
    fn tp1_tp2_leave_exact_twenty_percent_runner_for_both_sides() {
        for side in [Side::Long, Side::Short] {
            let mut position = position(side);
            let (tp1_bid, tp1_ask, tp2_bid, tp2_ask) = match side {
                Side::Long => (102.1, 102.2, 104.1, 104.2),
                Side::Short => (97.8, 97.9, 95.8, 95.9),
            };
            position.on_quote(
                quote(tp1_bid, tp1_ask, 0.5, None),
                CostModel::default(),
                PositionPolicy::default(),
            );
            assert_eq!(position.stage, PositionStage::AfterTp1);
            assert!((position.remaining_quantity - 6.0).abs() < EPSILON);
            position.on_quote(
                quote(tp2_bid, tp2_ask, 0.5, None),
                CostModel::default(),
                PositionPolicy::default(),
            );
            assert_eq!(position.stage, PositionStage::Runner);
            assert!((position.remaining_quantity - 2.0).abs() < EPSILON);
            match side {
                Side::Long => assert!(position.stop >= position.tp1),
                Side::Short => assert!(position.stop <= position.tp1),
            }
        }
    }

    #[test]
    fn stop_never_moves_backwards() {
        let mut position = position(Side::Long);
        let policy = PositionPolicy::default();
        let costs = CostModel::default();
        position.on_quote(quote(101.5, 101.6, 0.5, None), costs, policy);
        let raised = position.stop;
        position.on_quote(quote(100.8, 100.9, 2.0, Some(99.0)), costs, policy);
        assert!(position.stop >= raised);
    }

    #[test]
    fn runner_has_no_fixed_tp_and_exits_on_trailing_stop() {
        let mut position = position(Side::Long);
        let policy = PositionPolicy::default();
        let costs = CostModel::default();
        position.on_quote(quote(102.1, 102.2, 0.5, None), costs, policy);
        position.on_quote(quote(104.1, 104.2, 0.5, None), costs, policy);
        position.on_quote(quote(110.0, 110.1, 0.5, None), costs, policy);
        assert_eq!(position.stage, PositionStage::Runner);
        assert!(position.stop > 104.0);
        let stop = position.stop;
        let events = position.on_quote(quote(stop - 0.01, stop, 0.5, None), costs, policy);
        assert_eq!(position.stage, PositionStage::Closed);
        assert!(events.iter().any(|event| matches!(
            event,
            PositionEvent::Closed {
                reason: ExitReason::RunnerTrail,
                ..
            }
        )));
    }
}
