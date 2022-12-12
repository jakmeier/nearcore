//! Estimation functions for action costs, separated by send and exec.

use crate::estimator_context::EstimatorContext;
use crate::gas_cost::GasCost;
use crate::utils::action_send_cost;
use near_crypto::KeyType;
use near_primitives::transaction::{Action, CreateAccountAction, StakeAction};

fn stake_send(ctx: &mut EstimatorContext, sender_is_receiver: bool) -> GasCost {
    let stake_action = Action::Stake(StakeAction {
        stake: 5u128.pow(28),
        public_key: near_crypto::PublicKey::from_seed(KeyType::ED25519, "seed"),
    });
    action_send_cost(ctx, stake_action, sender_is_receiver)
}

pub(crate) fn stake_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    stake_send(ctx, true)
}

pub(crate) fn stake_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    stake_send(ctx, false)
}

fn create_account_send(ctx: &mut EstimatorContext, sender_is_receiver: bool) -> GasCost {
    // TODO: should also test account that doesn't exists, yet
    let stake_action = Action::CreateAccount(CreateAccountAction {});
    action_send_cost(ctx, stake_action, sender_is_receiver)
}

pub(crate) fn create_account_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    create_account_send(ctx, true)
}

pub(crate) fn create_account_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    create_account_send(ctx, false)
}
