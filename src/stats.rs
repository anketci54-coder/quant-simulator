
#[derive(Clone, Copy, Debug)]
pub struct OutcomeStats {
    pub wins: u64,
    pub losses: u64,
    pub gross_win_sum: f64,
    pub gross_loss_sum: f64,
}

impl OutcomeStats {
    pub fn samples(self) -> u64 {
        self.wins + self.losses
    }

    pub fn posterior_win_probability(self, alpha: f64, beta: f64) -> Option<f64> {
        if alpha <= 0.0 || beta <= 0.0 {
            return None;
        }
        Some((self.wins as f64 + alpha) / (self.samples() as f64 + alpha + beta))
    }

    pub fn average_win(self) -> Option<f64> {
        (self.wins > 0).then_some(self.gross_win_sum / self.wins as f64)
    }

    pub fn average_loss(self) -> Option<f64> {
        (self.losses > 0).then_some(self.gross_loss_sum.abs() / self.losses as f64)
    }

    pub fn net_expectancy(self, alpha: f64, beta: f64, round_trip_cost: f64) -> Option<f64> {
        let probability = self.posterior_win_probability(alpha, beta)?;
        let average_win = self.average_win()?;
        let average_loss = self.average_loss()?;
        Some(
            probability * average_win
                - (1.0 - probability) * average_loss
                - round_trip_cost.max(0.0),
        )
    }

    pub fn entry_allowed(
        self,
        minimum_samples: u64,
        alpha: f64,
        beta: f64,
        round_trip_cost: f64,
    ) -> bool {
        self.samples() >= minimum_samples
            && self
                .net_expectancy(alpha, beta, round_trip_cost)
                .is_some_and(|expectancy| expectancy > 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn beta_prior_prevents_overconfidence_on_tiny_samples() {
        let stats = OutcomeStats {
            wins: 1,
            losses: 0,
            gross_win_sum: 2.0,
            gross_loss_sum: 0.0,
        };
        assert_eq!(stats.posterior_win_probability(2.0, 2.0), Some(0.6));
        assert!(!stats.entry_allowed(30, 2.0, 2.0, 0.1));
    }

    #[test]
    fn positive_expectancy_requires_cost_adjusted_edge() {
        let profitable = OutcomeStats {
            wins: 18,
            losses: 12,
            gross_win_sum: 36.0,
            gross_loss_sum: 12.0,
        };
        let unprofitable = OutcomeStats {
            wins: 12,
            losses: 18,
            gross_win_sum: 12.0,
            gross_loss_sum: 27.0,
        };
        assert!(profitable.entry_allowed(30, 2.0, 2.0, 0.1));
        assert!(!unprofitable.entry_allowed(30, 2.0, 2.0, 0.1));
    }
}
