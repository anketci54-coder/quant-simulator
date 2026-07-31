use crate::model::Side;

const BPS_DIVISOR: f64 = 10_000.0;

#[derive(Clone, Copy, Debug)]
pub struct CostModel {
    /// Decimal rate: 0.0004 is 4 bps.
    pub entry_fee_rate: f64,
    /// Decimal rate: 0.0004 is 4 bps.
    pub exit_fee_rate: f64,
    pub expected_entry_slippage_bps: f64,
    pub expected_exit_slippage_bps: f64,
    /// Additional safety allowance applied to the exit trigger.
    pub safety_buffer_bps: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            entry_fee_rate: 0.0004,
            exit_fee_rate: 0.0004,
            expected_entry_slippage_bps: 1.0,
            expected_exit_slippage_bps: 2.0,
            safety_buffer_bps: 1.0,
        }
    }
}

impl CostModel {
    pub fn validate(self) -> bool {
        self.entry_fee_rate.is_finite()
            && self.exit_fee_rate.is_finite()
            && self.expected_entry_slippage_bps.is_finite()
            && self.expected_exit_slippage_bps.is_finite()
            && self.safety_buffer_bps.is_finite()
            && (0.0..0.01).contains(&self.entry_fee_rate)
            && (0.0..0.01).contains(&self.exit_fee_rate)
            && (0.0..=100.0).contains(&self.expected_entry_slippage_bps)
            && (0.0..=100.0).contains(&self.expected_exit_slippage_bps)
            && (0.0..=100.0).contains(&self.safety_buffer_bps)
    }

    pub fn estimated_entry_fill(self, side: Side, bid: f64, ask: f64) -> Option<f64> {
        if !self.validate() || bid <= 0.0 || ask < bid {
            return None;
        }
        let slippage = self.expected_entry_slippage_bps / BPS_DIVISOR;
        Some(match side {
            Side::Long => ask * (1.0 + slippage),
            Side::Short => bid * (1.0 - slippage),
        })
    }

    pub fn estimated_exit_fill(self, side: Side, bid: f64, ask: f64) -> Option<f64> {
        if !self.validate() || bid <= 0.0 || ask < bid {
            return None;
        }
        let slippage = self.expected_exit_slippage_bps / BPS_DIVISOR;
        Some(match side {
            Side::Long => bid * (1.0 - slippage),
            Side::Short => ask * (1.0 + slippage),
        })
    }

    pub fn estimated_stop_fill(self, side: Side, trigger_price: f64) -> Option<f64> {
        if !self.validate() || trigger_price <= 0.0 {
            return None;
        }
        let slippage = self.expected_exit_slippage_bps / BPS_DIVISOR;
        Some(match side {
            Side::Long => trigger_price * (1.0 - slippage),
            Side::Short => trigger_price * (1.0 + slippage),
        })
    }

    pub fn entry_fee(self, entry_fill: f64, quantity: f64) -> f64 {
        (entry_fill * quantity * self.entry_fee_rate).max(0.0)
    }

    pub fn exit_fee(self, exit_fill: f64, quantity: f64) -> f64 {
        (exit_fill * quantity * self.exit_fee_rate).max(0.0)
    }

    /// Returns the trigger price that should yield at least break-even after
    /// entry fee, expected exit fee/slippage, accumulated funding and a small
    /// safety allowance. `funding_cost` is positive when paid and negative when
    /// received.
    pub fn break_even_trigger(
        self,
        side: Side,
        entry_fill: f64,
        quantity: f64,
        funding_cost: f64,
    ) -> Option<f64> {
        if !self.validate() || entry_fill <= 0.0 || quantity <= 0.0 || !funding_cost.is_finite() {
            return None;
        }

        let funding_per_unit = funding_cost / quantity;
        let buffer = entry_fill * self.safety_buffer_bps / BPS_DIVISOR;
        let required_exit_fill = match side {
            Side::Long => {
                (entry_fill * (1.0 + self.entry_fee_rate) + funding_per_unit + buffer)
                    / (1.0 - self.exit_fee_rate)
            }
            Side::Short => {
                (entry_fill * (1.0 - self.entry_fee_rate) - funding_per_unit - buffer)
                    / (1.0 + self.exit_fee_rate)
            }
        };
        let exit_slippage = self.expected_exit_slippage_bps / BPS_DIVISOR;
        let trigger = match side {
            Side::Long => required_exit_fill / (1.0 - exit_slippage),
            Side::Short => required_exit_fill / (1.0 + exit_slippage),
        };
        (trigger > 0.0 && trigger.is_finite()).then_some(trigger)
    }

    pub fn net_pnl(
        self,
        side: Side,
        entry_fill: f64,
        exit_fill: f64,
        quantity: f64,
        allocated_entry_fee: f64,
        funding_cost: f64,
    ) -> f64 {
        let gross = (exit_fill - entry_fill) * quantity * side.direction();
        gross - allocated_entry_fee.max(0.0) - self.exit_fee(exit_fill, quantity) - funding_cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_adjusted_break_even_is_profitable_after_expected_costs() {
        let costs = CostModel::default();
        for side in [Side::Long, Side::Short] {
            let quantity = 2.0;
            let entry = 100.0;
            let funding = 0.03;
            let trigger = costs
                .break_even_trigger(side, entry, quantity, funding)
                .unwrap();
            let exit_slippage = costs.expected_exit_slippage_bps / BPS_DIVISOR;
            let exit_fill = match side {
                Side::Long => trigger * (1.0 - exit_slippage),
                Side::Short => trigger * (1.0 + exit_slippage),
            };
            let net = costs.net_pnl(
                side,
                entry,
                exit_fill,
                quantity,
                costs.entry_fee(entry, quantity),
                funding,
            );
            assert!(net > 0.0, "{side:?} net={net}");
        }
    }

    #[test]
    fn positive_funding_moves_long_and_short_break_even_in_opposite_directions() {
        let costs = CostModel::default();
        let long_without = costs
            .break_even_trigger(Side::Long, 100.0, 1.0, 0.0)
            .unwrap();
        let long_with = costs
            .break_even_trigger(Side::Long, 100.0, 1.0, 0.05)
            .unwrap();
        let short_without = costs
            .break_even_trigger(Side::Short, 100.0, 1.0, 0.0)
            .unwrap();
        let short_with = costs
            .break_even_trigger(Side::Short, 100.0, 1.0, 0.05)
            .unwrap();
        assert!(long_with > long_without);
        assert!(short_with < short_without);
    }
}
