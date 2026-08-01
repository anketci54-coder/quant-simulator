use crate::{
    cost::CostModel,
    model::{Candidate, Side},
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PositionSize {
    pub quantity: f64,
    pub margin: f64,
    pub risk: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct RiskLimits {
    pub risk_fraction: f64,
    pub allocation_fraction: f64,
    pub leverage: f64,
    pub step_size: f64,
    pub min_quantity: f64,
    /// Conservative fee + slippage allowance as a decimal fraction of price.
    pub expected_round_trip_cost_rate: f64,
}

pub fn size_candidate(
    balance: f64,
    candidate: &Candidate,
    limits: RiskLimits,
) -> Option<PositionSize> {
    if balance <= 0.0
        || candidate.price <= 0.0
        || candidate.stop_distance <= 0.0
        || limits.leverage <= 0.0
        || limits.step_size <= 0.0
        || limits.expected_round_trip_cost_rate < 0.0
    {
        return None;
    }
    let confidence = candidate.confidence.clamp(0.0, 1.0);
    let risk_budget = balance * limits.risk_fraction * confidence;
    let cost_per_unit = candidate.price * limits.expected_round_trip_cost_rate;
    let loss_per_unit = candidate.stop_distance + cost_per_unit;
    let risk_quantity = risk_budget / loss_per_unit;
    let allocation_scale = 0.35 + confidence * 0.35;
    let allocation_quantity =
        balance * limits.allocation_fraction * allocation_scale * limits.leverage / candidate.price;
    let liquidity_quantity = candidate.liquidity_notional.max(0.0) * 0.05 / candidate.price;
    let raw = risk_quantity
        .min(allocation_quantity)
        .min(liquidity_quantity);
    // Five lot steps make the 40/40/20 split exactly executable without
    // inventing fractional exchange quantities.
    let allocation_step = limits.step_size * 5.0;
    let quantity = (raw / allocation_step).floor() * allocation_step;
    if quantity < limits.min_quantity.max(allocation_step) {
        return None;
    }
    Some(PositionSize {
        quantity,
        margin: quantity * candidate.price / limits.leverage,
        risk: quantity * loss_per_unit,
    })
}

/// Remaining portfolio risk is the loss that would actually be realized at
/// the current stop after fees, expected stop slippage and funding. A stop
/// that already locks net profit contributes zero downside risk.
pub fn cost_adjusted_risk_to_stop(
    side: Side,
    entry_fill: f64,
    stop_trigger: f64,
    remaining_quantity: f64,
    entry_fee_remaining: f64,
    funding_cost: f64,
    costs: CostModel,
) -> Option<f64> {
    if entry_fill <= 0.0
        || stop_trigger <= 0.0
        || remaining_quantity <= 0.0
        || entry_fee_remaining < 0.0
        || !funding_cost.is_finite()
    {
        return None;
    }
    let stop_fill = costs.estimated_stop_fill(side, stop_trigger)?;
    let net_pnl = costs.net_pnl(
        side,
        entry_fill,
        stop_fill,
        remaining_quantity,
        entry_fee_remaining,
        funding_cost,
    );
    Some((-net_pnl).max(0.0))
}

#[cfg(test)]
mod tests {
    use crate::model::{FeatureSnapshot, Side};

    use super::*;

    #[test]
    fn sizing_is_bounded_by_risk_allocation_and_liquidity() {
        let candidate = Candidate {
            symbol: "TESTUSDT".to_string(),
            side: Side::Long,
            price: 100.0,
            score: 80.0,
            confidence: 0.8,
            stop_distance: 1.0,
            liquidity_notional: 20_000.0,
            observed_at: 1,
            features: FeatureSnapshot::default(),
        };
        let size = size_candidate(
            10_000.0,
            &candidate,
            RiskLimits {
                risk_fraction: 0.005,
                allocation_fraction: 0.10,
                leverage: 3.0,
                step_size: 0.001,
                min_quantity: 0.001,
                expected_round_trip_cost_rate: 0.001,
            },
        )
        .unwrap();
        assert!(size.risk <= 40.0 + f64::EPSILON);
        assert!(size.margin <= 1_000.0 + f64::EPSILON);
    }

    #[test]
    fn profit_lock_contributes_zero_portfolio_downside_risk() {
        let costs = CostModel::default();
        let quantity = 10.0;
        let entry = 100.0;
        let entry_fee = costs.entry_fee(entry, quantity);
        let break_even = costs
            .break_even_trigger(Side::Long, entry, quantity, 0.0)
            .unwrap();
        let risk = cost_adjusted_risk_to_stop(
            Side::Long,
            entry,
            break_even + 0.25,
            quantity,
            entry_fee,
            0.0,
            costs,
        )
        .unwrap();
        assert_eq!(risk, 0.0);
    }
}
