//! State viewer functions to read gas profile information from execution
//! outcomes stored in RocksDB.

use anyhow::{bail, Context};
use near_chain::{ChainStore, ChainStoreAccess};
use near_primitives::hash::CryptoHash;
use near_primitives::profile::Cost;
use near_primitives::receipt::{ActionReceipt, DataReceiver, Receipt, ReceiptEnum};
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::parameter_table::ParameterTable;
use near_primitives::transaction::{
    Action, ExecutionOutcome, ExecutionOutcomeWithIdAndProof, FunctionCallAction,
};
use near_primitives::types::{AccountId, BlockHeight, Gas};
use near_primitives::version::ProtocolVersion;
use near_primitives_core::parameter::Parameter;
use near_store::{ShardUId, Store, Trie, TrieCache, TrieCachingStorage, TrieConfig};
use nearcore::NearConfig;
use node_runtime::config::{total_prepaid_exec_fees, total_send_fees, RuntimeConfig};
use std::collections::BTreeMap;
use tracing::{debug, error};

pub(crate) struct GasFeeCounters {
    counters: BTreeMap<Parameter, u64>,
}

pub(crate) fn extract_gas_counters(
    outcome: &ExecutionOutcome,
    runtime_config: &RuntimeConfig,
) -> Option<GasFeeCounters> {
    match &outcome.metadata {
        near_primitives::transaction::ExecutionMetadata::V1 => None,
        near_primitives::transaction::ExecutionMetadata::V2(meta_data) => {
            let mut counters = BTreeMap::new();

            for param in Parameter::ext_costs() {
                match param.cost().unwrap_or_else(|| panic!("ext cost {param} must have a cost")) {
                    Cost::ExtCost { ext_cost_kind } => {
                        let parameter_value =
                            ext_cost_kind.value(&runtime_config.wasm_config.ext_costs);
                        let gas = meta_data.get_ext_cost(ext_cost_kind);
                        if parameter_value != 0 && gas != 0 {
                            assert_eq!(
                                0,
                                gas % parameter_value,
                                "invalid gas profile for given config"
                            );
                            let counter = gas / parameter_value;
                            *counters.entry(*param).or_default() += counter;
                        }
                    }
                    _ => unreachable!("{param} must be ExtCost"),
                };
            }

            let num_wasm_ops = meta_data[Cost::WasmInstruction]
                / runtime_config.wasm_config.regular_op_cost as u64;
            if num_wasm_ops != 0 {
                *counters.entry(Parameter::WasmRegularOpCost).or_default() += num_wasm_ops;
            }

            // TODO: Action costs should also be included.
            // This is tricky, however. From just the gas numbers in the profile
            // we cannot know the cost is split to parameters. Because base and byte
            // costs are all merged. Same for different type of access keys.
            // The only viable way right now is go through each action separately and
            // recompute the gas cost from scratch. For promises in function
            // calls that includes looping through outgoing promises and again
            // recomputing the gas costs.
            // And of course one has to consider that some actions will be SIR
            // and some will not be.
            //
            // For now it is not clear if implementing this is even worth it.
            // Alternatively, we could also make the profile data more detailed.

            // special case: value return, this can be done easily
            let num_value_return = meta_data[Cost::ActionCost {
                action_cost_kind: near_primitives::config::ActionCosts::value_return,
            }] / 2
                / runtime_config
                    .transaction_costs
                    .data_receipt_creation_config
                    .cost_per_byte
                    .exec_fee() as u64;
            if num_value_return != 0 {
                *counters.entry(Parameter::DataReceiptCreationPerByteExecution).or_default() +=
                    num_value_return;
                *counters.entry(Parameter::DataReceiptCreationPerByteSendNotSir).or_default() +=
                    num_value_return;
            }

            Some(GasFeeCounters { counters })
        }
    }
}

pub(crate) struct GasParameterChangeChecker {
    chain_store: ChainStore,
    config_store: RuntimeConfigStore,
    tries: [Trie; 4],
    new_config: RuntimeConfig,
    new_params_table: ParameterTable,
    gas_limit: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GasCostChange {
    Cheaper { change: Gas },
    Equal,
    MoreExpensiveButOk { change: Gas },
    MoreExpensiveAboveAttachedGas { change: Gas, above_attached: Gas },
    MoreExpensiveAboveGasLimit { change: Gas, above_attached: Gas, above_limit: Gas },
}

impl GasParameterChangeChecker {
    pub(crate) fn new(
        store: &Store,
        near_config: &NearConfig,
        gas_limit: u64,
        new_params_table: ParameterTable,
    ) -> anyhow::Result<Self> {
        let chain_store = ChainStore::new(
            store.clone(),
            near_config.genesis.config.genesis_height,
            !near_config.client_config.archive,
        );

        let config_store = RuntimeConfigStore::new(None);
        let new_config = RuntimeConfig::new(&new_params_table)?;

        let newest_block_hash = chain_store.final_head()?.last_block_hash;
        let newest_block = chain_store.get_block(&newest_block_hash)?;
        let state_roots = vec![
            newest_block.chunks()[0].prev_state_root(),
            newest_block.chunks()[1].prev_state_root(),
            newest_block.chunks()[2].prev_state_root(),
            newest_block.chunks()[3].prev_state_root(),
        ];

        let trie_config: TrieConfig = Default::default();

        let trie_storage = |shard_id: u32| {
            let shard_uid = ShardUId { version: 1, shard_id };
            let shard_cache = TrieCache::new(&trie_config, shard_uid, false);
            let trie_storage =
                TrieCachingStorage::new(store.clone(), shard_cache, shard_uid, false, None);
            Box::new(trie_storage)
        };

        let tries = [
            Trie::new(trie_storage(0), state_roots[0], None),
            Trie::new(trie_storage(1), state_roots[1], None),
            Trie::new(trie_storage(2), state_roots[2], None),
            Trie::new(trie_storage(3), state_roots[3], None),
        ];
        Ok(Self { config_store, tries, gas_limit, new_config, new_params_table, chain_store })
    }

    pub(crate) fn collect_gas_changes_in_block(
        &self,
        height: BlockHeight,
        stats: &mut ParamChangeStats,
    ) -> Result<(), anyhow::Error> {
        let block_hash = match self.chain_store.get_block_hash_by_height(height) {
            Ok(hash) => hash,
            Err(near_chain::Error::DBNotFoundErr(..)) => {
                stats.num_missing_blocks += 1;
                return Ok(());
            }
            Err(e) => bail!("unexpected error when looking up block hash {e}"),
        };
        let block = self.chain_store.get_block(&block_hash)?;
        let block_protocol_version = block.header().latest_protocol_version();
        let block_runtime_config = self.config_store.get_config(block_protocol_version);
        Ok(for chunk_header in block.chunks().iter() {
            let chunk = self.chain_store.get_chunk(&chunk_header.chunk_hash())?;
            for receipt in chunk.receipts().iter() {
                let receipt_id = receipt.receipt_id;
                for outcome in self.chain_store.get_outcomes_by_id(&receipt_id)? {
                    let trie = &self.tries[chunk_header.shard_id() as usize];

                    self.check_outcome_change(
                        receipt,
                        &outcome,
                        block_runtime_config,
                        block_protocol_version,
                        trie,
                        stats,
                    );
                }
            }
        })
    }

    fn check_outcome_change(
        &self,
        receipt: &Receipt,
        outcome: &ExecutionOutcomeWithIdAndProof,
        block_runtime_config: &RuntimeConfig,
        block_protocol_version: u32,
        trie: &Trie,
        stats: &mut ParamChangeStats,
    ) {
        const MAX_RECEIPTS_PRINTED: usize = 3;
        let change = self.function_call_gas_change(
            receipt,
            &outcome,
            block_runtime_config,
            block_protocol_version,
            trie,
        );
        match change {
            Err(err) => {
                stats.num_replay_errors += 1;
                error!(target: "state_viewer", "{err}");
            }
            Ok(None) => {
                // not a function call, just continue
            }
            Ok(Some(GasCostChange::Equal)) => stats.num_equal += 1,
            Ok(Some(GasCostChange::Cheaper { change })) => {
                if stats.cheaper_receipts.len() < MAX_RECEIPTS_PRINTED {
                    stats.cheaper_receipts.push(receipt.receipt_id);
                }
                if let Some(counters) = stats.affected_accounts.get_mut(&receipt.receiver_id) {
                    counters.2 += 1;
                }
                stats.total_gas_cheaper += change;
                stats.num_cheaper += 1;
            }
            Ok(Some(GasCostChange::MoreExpensiveButOk { change })) => {
                stats.num_more_expensive += 1;
                stats.total_gas_more_expensive += change;
            }
            Ok(Some(GasCostChange::MoreExpensiveAboveAttachedGas { change, above_attached })) => {
                stats.num_avoidable_err += 1;
                stats.num_more_expensive += 1;
                stats.total_gas_more_expensive += change;

                stats.affected_accounts.entry(receipt.receiver_id.clone()).or_default().0 += 1;
                if stats.avoidable_err_receipts.len() < MAX_RECEIPTS_PRINTED {
                    stats.avoidable_err_receipts.push(receipt.receipt_id);
                }
                debug!("{} exceeds attached gas by {}", receipt.receipt_id, above_attached);
            }
            Ok(Some(GasCostChange::MoreExpensiveAboveGasLimit {
                change,
                above_attached,
                above_limit,
            })) => {
                stats.num_unavoidable_err += 1;
                stats.num_more_expensive += 1;
                stats.total_gas_more_expensive += change;

                stats.affected_accounts.entry(receipt.receiver_id.clone()).or_default().1 += 1;
                if stats.unavoidable_err_receipts.len() < MAX_RECEIPTS_PRINTED {
                    stats.unavoidable_err_receipts.push(receipt.receipt_id);
                }
                debug!("{} exceeds attached gas by {}", receipt.receipt_id, above_attached);
                debug!("{} exceeds gas limit by {}", receipt.receipt_id, above_limit);
            }
        }
    }

    fn function_call_gas_change(
        &self,
        receipt: &Receipt,
        outcome: &ExecutionOutcomeWithIdAndProof,
        block_runtime_config: &RuntimeConfig,
        block_protocol_version: ProtocolVersion,
        trie: &Trie,
    ) -> anyhow::Result<Option<GasCostChange>> {
        let receipt_id = receipt.receipt_id;
        let gas_burnt = outcome.outcome_with_id.outcome.gas_burnt;
        let gas_attached: u64 = fn_calls(receipt).into_iter().flatten().map(|func| func.gas).sum();
        if gas_attached == 0 {
            // Not a fn call, skip.
            return Ok(None);
        }
        let gas_profile =
            extract_gas_counters(&outcome.outcome_with_id.outcome, block_runtime_config)
                .with_context(|| format!("missing gas profile {receipt_id:?}"))?;

        let gas_pre_burned =
            self.new_config.transaction_costs.action_receipt_creation_config.exec_fee()
                + total_prepaid_exec_fees(
                    &self.new_config.transaction_costs,
                    &as_action_receipt(receipt).unwrap().actions,
                    &receipt.receiver_id,
                    block_protocol_version,
                )?;
        let gas_available = gas_attached + gas_pre_burned;

        let outgoing_send_gas: Gas = outcome.outcome_with_id.outcome.receipt_ids.iter().try_fold(
            0,
            |acc, outgoing_receipt_id| {
                let maybe_outgoing_receipt = self
                    .chain_store
                    .get_receipt(outgoing_receipt_id)
                    .context("DB err for outgoing receipt")?;
                if maybe_outgoing_receipt.is_none() {}
                let outgoing_receipt = match maybe_outgoing_receipt {
                    Some(receipt) if receipt.predecessor_id.is_system() => {
                        return Ok::<Gas, anyhow::Error>(acc)
                    }
                    None => return Ok(acc),
                    Some(receipt) => receipt,
                };
                let sender_is_receiver =
                    outgoing_receipt.predecessor_id == outgoing_receipt.receiver_id;
                match &outgoing_receipt.receipt {
                    ReceiptEnum::Action(action_receipt) => {
                        let action_cost = self
                            .new_config
                            .transaction_costs
                            .action_receipt_creation_config
                            .send_fee(sender_is_receiver)
                            + total_send_fees(
                                &self.new_config.transaction_costs,
                                sender_is_receiver,
                                &action_receipt.actions,
                                &outgoing_receipt.receiver_id,
                                block_protocol_version,
                            )
                            .context("fee calculation must not fail")?;
                        let data_cost =
                            self.action_receipt_data_cost(action_receipt, trie, receipt)?;
                        Ok(acc + action_cost + data_cost)
                    }
                    ReceiptEnum::Data(_data_receipt) => Ok(acc),
                }
            },
        )?;

        let new_gas =
            gas_profile.gas_required(&self.new_params_table) + gas_pre_burned + outgoing_send_gas;

        debug!("{receipt_id} new_gas={new_gas}, gas_available={gas_available}, gas_attached={gas_attached}, gas_pre_burned={gas_pre_burned}, gas_burnt={gas_burnt}");

        let change = match new_gas.cmp(&gas_burnt) {
            std::cmp::Ordering::Equal => GasCostChange::Equal,
            std::cmp::Ordering::Greater if new_gas <= gas_available => {
                GasCostChange::MoreExpensiveButOk { change: new_gas - gas_burnt }
            }
            std::cmp::Ordering::Greater if new_gas <= self.gas_limit => {
                let percent = ((new_gas as f64 / gas_burnt as f64) - 1.0) * 100.0;
                let gas_limit = self.gas_limit;
                debug!(
                    "{receipt_id:?} OK but exceeds old gas burnt by {percent:.2}% ({gas_limit} > {new_gas} > {gas_available})"
                );

                GasCostChange::MoreExpensiveAboveAttachedGas {
                    change: new_gas - gas_burnt,
                    above_attached: new_gas - gas_available,
                }
            }
            std::cmp::Ordering::Greater => GasCostChange::MoreExpensiveAboveGasLimit {
                change: new_gas - gas_burnt,
                above_attached: new_gas - gas_available,
                above_limit: new_gas - self.gas_limit,
            },
            std::cmp::Ordering::Less => GasCostChange::Cheaper { change: gas_burnt - new_gas },
        };
        Ok(Some(change))
    }

    /// gas cost for data dependencies of a new action receipt
    fn action_receipt_data_cost(
        &self,
        action_receipt: &ActionReceipt,
        trie: &Trie,
        receipt: &Receipt,
    ) -> anyhow::Result<Gas> {
        action_receipt.output_data_receivers.iter().try_fold(
            0,
            |acc, DataReceiver { data_id, receiver_id }| {
                let data = near_store::get_received_data(trie, receiver_id, *data_id)
                    .context("data must be received")?;
                let sender_is_receiver = receipt.receiver_id == *receiver_id;
                let data_config = &self.new_config.transaction_costs.data_receipt_creation_config;
                let cost = data_config.base_cost.exec_fee()
                    + data_config.base_cost.send_fee(sender_is_receiver)
                    + data
                        .as_ref()
                        .and_then(|data| data.data.as_ref().map(|d| d.len() as u64))
                        .unwrap_or(acc)
                        * (data_config.cost_per_byte.exec_fee()
                            + data_config.cost_per_byte.send_fee(sender_is_receiver));
                Ok(acc + cost)
            },
        )
    }
}

fn as_action_receipt(receipt: &Receipt) -> Option<&ActionReceipt> {
    if let ReceiptEnum::Action(action_receipt) = &receipt.receipt {
        Some(action_receipt)
    } else {
        None
    }
}

fn fn_calls(receipt: &Receipt) -> Option<impl Iterator<Item = &FunctionCallAction>> {
    Some(as_action_receipt(receipt)?.actions.iter().flat_map(|a| match a {
        Action::FunctionCall(fn_call_action) => Some(fn_call_action),
        _ => None,
    }))
}

impl GasFeeCounters {
    pub(crate) fn gas_required(&self, params: &ParameterTable) -> Gas {
        self.counters
            .iter()
            .map(|(param, counter)| params.get(*param).unwrap().as_u64().unwrap() * counter)
            .sum()
    }
}

impl std::fmt::Display for GasFeeCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (param, counter) in self.counters.iter() {
            writeln!(f, "{param:<48} {counter:>16}")?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct ParamChangeStats {
    pub num_equal: u64,
    pub num_avoidable_err: u64,
    pub num_unavoidable_err: u64,
    pub num_cheaper: u64,
    pub num_more_expensive: u64,
    pub total_gas_cheaper: u64,
    pub total_gas_more_expensive: u64,
    pub num_replay_errors: u64,
    pub num_missing_blocks: u64,
    pub affected_accounts: BTreeMap<AccountId, (u32, u32, u32)>,
    // store a few samples receipts for further analysis
    pub cheaper_receipts: Vec<CryptoHash>,
    pub avoidable_err_receipts: Vec<CryptoHash>,
    pub unavoidable_err_receipts: Vec<CryptoHash>,
}

impl ParamChangeStats {
    pub(crate) fn single_line_summary(&self) -> String {
        let unavoidable_err = self.num_unavoidable_err;
        let avoidable_err = self.num_avoidable_err;
        let ok = self.ok();
        format!("({unavoidable_err}/{avoidable_err}/{ok}) (above_gas_limit/above old_gas_burnt/ok)")
    }

    fn ok(&self) -> u64 {
        self.num_cheaper + self.num_equal + self.num_more_expensive
            - self.num_avoidable_err
            - self.num_unavoidable_err
    }
}

impl std::fmt::Display for ParamChangeStats {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_equal = self.num_equal;
        let num_avoidable_err = self.num_avoidable_err;
        let num_unavoidable_err = self.num_unavoidable_err;
        let num_cheaper = self.num_cheaper;
        let num_more_expensive = self.num_more_expensive;
        let total_gas_cheaper = self.total_gas_cheaper;
        let total_gas_more_expensive = self.total_gas_more_expensive;
        let num_replay_errors = self.num_replay_errors;
        let num_missing_blocks = self.num_missing_blocks;

        let num_ok = self.ok();
        let avg_cheaper = total_gas_cheaper as f64 / num_cheaper as f64 / 10e12;
        let avg_expensive = total_gas_more_expensive as f64 / num_more_expensive as f64 / 10e12;

        writeln!(out, "{num_cheaper:<12} became cheaper, {avg_cheaper:.3} Tgas on average")?;
        writeln!(out, "{num_equal:<12} same amount of gas")?;
        writeln!(
            out,
            "{num_more_expensive:<12} more expensive, {avg_expensive:.3} Tgas on average"
        )?;
        writeln!(out)?;
        writeln!(out, "{num_unavoidable_err:<12} exceeding gas limit")?;
        writeln!(out, "{num_avoidable_err:<12} need more gas attached")?;
        writeln!(out, "{num_ok:<12} ok")?;
        writeln!(out)?;

        if !self.affected_accounts.is_empty() {
            writeln!(
                out,
                "{:32} {:12}/{:12}   {}",
                "List of broken receivers", "avoidable", "unavoidable", "cheaper"
            )?;
        }
        for (account, (avoidable, unavoidable, cheaper)) in &self.affected_accounts {
            writeln!(out, "{account:<32} {avoidable:>12}/{unavoidable:<12}   {cheaper}")?;
        }

        writeln!(out)?;
        writeln!(out, "Unavoidable error receipts:")?;
        for hash in &self.unavoidable_err_receipts {
            writeln!(out, "{hash}")?;
        }
        writeln!(out)?;
        writeln!(out, "Avoidable error receipts:")?;
        for hash in &self.avoidable_err_receipts {
            writeln!(out, "{hash}")?;
        }
        writeln!(out)?;
        writeln!(out, "Cheaper receipts:")?;
        for hash in &self.cheaper_receipts {
            writeln!(out, "{hash}")?;
        }

        writeln!(out)?;
        writeln!(out, "{num_missing_blocks:3} missing blocks")?;
        writeln!(out, "{num_replay_errors:3} replay errors")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::profile::ProfileData;
    use near_primitives::transaction::ExecutionMetadata;
    use near_primitives::types::AccountId;
    use node_runtime::config::RuntimeConfig;

    #[test]
    fn test_extract_gas_counters() {
        let config = RuntimeConfig::test();
        let costs = [
            (Parameter::WasmStorageWriteBase, 137),
            (Parameter::WasmStorageWriteKeyByte, 4629),
            (Parameter::WasmStorageWriteValueByte, 2246),
            // note: actions are not included in profile, yet
            (Parameter::ActionDeployContractExecution, 2 * 184765750000),
            (Parameter::ActionDeployContractSendSir, 2 * 184765750000),
            (Parameter::ActionDeployContractPerByteSendSir, 1024 * 6812999),
            (Parameter::ActionDeployContractPerByteExecution, 1024 * 64572944),
            (Parameter::WasmRegularOpCost, 7000),
        ];

        let outcome = create_execution_outcome(&costs, &config);
        let profile = extract_gas_counters(&outcome, &config).expect("no counters returned");

        insta::assert_display_snapshot!(profile);
    }

    fn create_execution_outcome(
        costs: &[(Parameter, u64)],
        config: &RuntimeConfig,
    ) -> ExecutionOutcome {
        let mut gas_burnt = 0;
        let mut profile_data = ProfileData::new();
        for &(parameter, value) in costs {
            match parameter.cost() {
                Some(Cost::ExtCost { ext_cost_kind }) => {
                    let gas = value * ext_cost_kind.value(&config.wasm_config.ext_costs);
                    profile_data.add_ext_cost(ext_cost_kind, gas);
                    gas_burnt += gas;
                }
                Some(Cost::WasmInstruction) => {
                    let gas = value * config.wasm_config.regular_op_cost as u64;
                    profile_data[Cost::WasmInstruction] += gas;
                    gas_burnt += gas;
                }
                // Multiplying for actions isn't possible because costs can be
                // split into multiple parameters. Caller has to specify exact
                // values.
                Some(Cost::ActionCost { action_cost_kind }) => {
                    profile_data.add_action_cost(action_cost_kind, value);
                    gas_burnt += value;
                }
                _ => unimplemented!(),
            }
        }
        let metadata = ExecutionMetadata::V2(profile_data);
        let account_id: AccountId = "alice.near".to_owned().try_into().unwrap();
        ExecutionOutcome {
            logs: vec![],
            receipt_ids: vec![],
            gas_burnt,
            tokens_burnt: 0,
            executor_id: account_id.clone(),
            status: near_primitives::transaction::ExecutionStatus::SuccessValue(vec![]),
            metadata,
        }
    }
}
