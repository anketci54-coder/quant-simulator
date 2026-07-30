use crate::model::Candidate;

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
    {
        return None;
    }
    let confidence = candidate.confidence.clamp(0.0, 1.0);
    let risk_budget = balance * limits.risk_fraction * confidence;
    let risk_quantity = risk_budget / candidate.stop_distance;
    let allocation_scale = 0.35 + confidence * 0.35;
    let allocation_quantity =
        balance * limits.allocation_fraction * allocation_scale * limits.leverage / candidate.price;
    let liquidity_quantity = candidate.liquidity_notional.max(0.0) * 0.05 / candidate.price;
    let raw = risk_quantity
        .min(allocation_quantity)
        .min(liquidity_quantity);
    let quantity = (raw / limits.step_size).floor() * limits.step_size;
    if quantity < limits.min_quantity {
        return None;
    }
    Some(PositionSize {
        quantity,
        margin: quantity * candidate.price / limits.leverage,
        risk: quantity * candidate.stop_distance,
    })
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
            },
        )
        .unwrap();
        assert!(size.risk <= 40.0 + f64::EPSILON);
        assert!(size.margin <= 630.0 + f64::EPSILON);
        assert!(size.quantity * candidate.price <= 1_000.0 + f64::EPSILON);
    }
}
