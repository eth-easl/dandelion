use composition::{CompositionTemplate, MetaApplication, Registry};
use dandelion_commons::{
    err_dandelion, DandelionError, DandelionResult, FunctionId, FunctionRegistryError,
};
use memory::{context::ContextTrait, DataItem, DataSet, ItemData};
use std::sync::Arc;

/// Input set of a planner with one item per data input of the application, in application order.
/// Each item is named after the set in the applying composition, has its position as key and holds
/// three little endian u64: the number of items in the set, the summed size in bytes of the items
/// whose size is known, and the number of items whose size is not yet known (IO items whose request
/// has not completed successfully yet, which do not contribute to the summed size).
pub const INPUT_METADATA_SET: &str = "__dandelion_metadata_in";
/// Input set of a planner with one empty item per output of the application, in application order.
/// Each item is named after the set in the applying composition and has its position as key.
pub const OUTPUT_METADATA_SET: &str = "__dandelion_metadata_out";
/// Name of the single output set of a planner, holding one item with the source of the composition
/// to run in place of the metafunction.
pub const COMPOSITION_SET: &str = "composition";

/// Builds the input metadata set for the given data inputs. Only uses what the platform already
/// knows about the sets, so remote items are not resolved.
pub fn input_metadata_set(names: &[String], sets: &[DataSet]) -> DandelionResult<DataSet> {
    DataSet::from_keyed_bytes(
        names
            .iter()
            .zip(sets)
            .enumerate()
            .map(|(key, (name, set))| {
                let (known_size, unknown_size_count) =
                    set.items.iter().fold((0, 0), |(known, unknown), item| {
                        match known_size(item) {
                            Some(size) => (known + size, unknown),
                            None => (known, unknown + 1),
                        }
                    });
                let mut data = (set.len() as u64).to_le_bytes().to_vec();
                data.extend_from_slice(&(known_size as u64).to_le_bytes());
                data.extend_from_slice(&(unknown_size_count as u64).to_le_bytes());
                (name.clone(), key as u32, data)
            }),
    )
}

/// The size of the item if it is already known. For IO items it is only known once their request
/// has completed successfully, possibly for another consumer, as they share the resolution.
fn known_size(item: &DataItem) -> Option<usize> {
    match &item.data {
        ItemData::Io(io) => match io.resolved.get() {
            Some(Ok(contexts)) => contexts.get(io.set_index).map(|ctx| ctx.size()),
            _ => None,
        },
        _ => Some(item.size()),
    }
}

/// Builds the output metadata set for the given outputs.
pub fn output_metadata_set(names: &[String]) -> DandelionResult<DataSet> {
    DataSet::from_keyed_bytes(
        names
            .iter()
            .enumerate()
            .map(|(key, name)| (name.clone(), key as u32, vec![])),
    )
}

/// Reads the composition source from the output sets of a planner.
pub fn read_composition(planner_output: &[Arc<Vec<DataItem>>]) -> DandelionResult<String> {
    let item = match planner_output.first().map(|set| set.as_slice()) {
        Some([item]) => item,
        _ => {
            return err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::InvalidPlan(format!(
                    "Planner needs to return exactly one item in its '{}' set",
                    COMPOSITION_SET
                )),
            ))
        }
    };
    let local = item.local_ref()?;
    let mut bytes = vec![0u8; local.pos.size];
    local.ctx.read(local.pos.offset, &mut bytes)?;
    match String::from_utf8(bytes) {
        Ok(composition) => Ok(composition),
        Err(_) => err_dandelion!(DandelionError::FunctionRegistry(
            FunctionRegistryError::InvalidPlan(String::from(
                "Composition returned by the planner is not valid UTF-8"
            )),
        )),
    }
}

/// Parses the composition returned by the planner of a metafunction and checks that it takes
/// the data inputs and produces the outputs of the application.
pub fn parse_plan<R: Registry>(
    registry: &R,
    function_id: &FunctionId,
    plan: &str,
    application: &MetaApplication,
) -> DandelionResult<(FunctionId, CompositionTemplate)> {
    let invalid_plan = |reason: String| {
        err_dandelion!(DandelionError::FunctionRegistry(
            FunctionRegistryError::InvalidPlan(format!(
                "Composition returned by the planner of {} {}",
                function_id, reason
            )),
        ))
    };
    let mut templates = match CompositionTemplate::parse(plan, registry) {
        Ok(templates) => templates,
        Err(err) => return invalid_plan(format!("failed to parse: {}", err.error)),
    };
    let num_compositions = templates.len();
    let (plan_id, template, metadata) = match templates.pop() {
        Some(parsed) if num_compositions == 1 => parsed,
        _ => {
            return invalid_plan(format!(
                "has to contain exactly one composition, found {}",
                num_compositions
            ))
        }
    };
    if metadata.input_sets.len() != application.input_names.len()
        || metadata.output_sets.len() != application.output_names.len()
    {
        return invalid_plan(format!(
            "takes {} input sets and returns {} output sets instead of {} and {}",
            metadata.input_sets.len(),
            metadata.output_sets.len(),
            application.input_names.len(),
            application.output_names.len()
        ));
    }
    Ok((plan_id, template))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dandelion_commons::SystemFunction;
    use memory::{
        context::{read_only::ReadOnlyContext, Context},
        IoData,
    };
    use tokio::sync::OnceCell;

    fn bytes_set(items: &[(&str, &[u8])]) -> DataSet {
        DataSet::from_bytes(
            items
                .iter()
                .map(|(name, data)| (name.to_string(), data.to_vec())),
        )
        .unwrap()
    }

    fn read_bytes(item: &DataItem) -> Vec<u8> {
        let local = item.local_ref().unwrap();
        let mut bytes = vec![0u8; local.pos.size];
        local.ctx.read(local.pos.offset, &mut bytes).unwrap();
        bytes
    }

    /// Decodes an item of the input metadata set into (item count, known size, unknown size count).
    fn read_metadata(item: &DataItem) -> (u64, u64, u64) {
        let bytes = read_bytes(item);
        assert_eq!(bytes.len(), 24, "metadata item holds three u64");
        let field = |i: usize| u64::from_le_bytes(bytes[i * 8..(i + 1) * 8].try_into().unwrap());
        (field(0), field(1), field(2))
    }

    fn assert_invalid_plan<T: std::fmt::Debug>(result: DandelionResult<T>) {
        match result {
            Err(err) => assert!(
                matches!(
                    err.error,
                    DandelionError::FunctionRegistry(FunctionRegistryError::InvalidPlan(_))
                ),
                "expected InvalidPlan, got {:?}",
                err.error
            ),
            Ok(value) => panic!("expected InvalidPlan, got Ok({:?})", value),
        }
    }

    #[test]
    fn input_metadata_describes_each_input_in_application_order() {
        let names = vec![String::from("A"), String::from("B")];
        let sets = [
            bytes_set(&[("a0", b"abc"), ("a1", b"defgh")]),
            DataSet::default(),
        ];

        let metadata = input_metadata_set(&names, &sets).unwrap();

        let items: Vec<_> = metadata
            .items
            .iter()
            .map(|i| (i.ident.as_str(), i.key))
            .collect();
        assert_eq!(items, [("A", 0), ("B", 1)]);
        // 2 items -> len of 2
        // "abc" -> 3B, "defgh" -> 5B
        // 0 items of unknown size (i.e. no unresolved IoData)
        assert_eq!(read_metadata(&metadata.items[0]), (2, 8, 0));
        // empty DataSet
        assert_eq!(
            read_metadata(&metadata.items[1]),
            (0, 0, 0),
            "empty input still described"
        );
    }

    fn io_item(
        request: &DataItem,
        resolved: Option<DandelionResult<Vec<Arc<Context>>>>,
        set_index: usize,
    ) -> DataItem {
        DataItem::new_io(
            request.ident.clone(),
            request.key,
            IoData {
                original_data: Box::new(request.data.clone()),
                resolved: Arc::new(OnceCell::new_with(resolved)),
                function: SystemFunction::HTTP,
                set_index,
            },
        )
    }

    fn context(bytes: &[u8]) -> Arc<Context> {
        Arc::new(ReadOnlyContext::from_boxed(bytes.to_vec().into_boxed_slice()).unwrap())
    }

    #[test]
    fn input_metadata_counts_only_unresolved_io_items_as_unknown_size() {
        let local = bytes_set(&[("local", b"abcd"), ("request", b"GET /something")]);
        let request = &local.items[1];
        // header and body output of the request, the item is the body (set index 1)
        let response = vec![context(b"HTTP/1.1 200 OK"), context(b"0123456789")];
        let failed = err_dandelion!(DandelionError::MemcachedError);
        let set = DataSet::from_items(Arc::new(vec![
            local.items[0].clone(),
            io_item(request, None, 0),
            io_item(request, Some(Ok(response)), 1),
            io_item(request, Some(failed), 0),
        ]));

        let metadata = input_metadata_set(&[String::from("A")], &[set]).unwrap();

        // 4 items
        // "abcd" -> 4B, resolved body "0123456789" -> 10B
        // the not yet resolved and the failed IO item are of unknown size
        assert_eq!(read_metadata(&metadata.items[0]), (4, 14, 2));
    }

    #[test]
    fn output_metadata_has_one_empty_item_per_output_in_order() {
        let names = vec![String::from("X"), String::from("Y")];

        let metadata = output_metadata_set(&names).unwrap();

        let items: Vec<_> = metadata
            .items
            .iter()
            .map(|i| (i.ident.as_str(), i.key, i.size()))
            .collect();
        assert_eq!(items, [("X", 0, 0), ("Y", 1, 0)]);
    }

    #[test]
    fn read_composition_returns_the_planned_source() {
        let source = "composition Plan(A) => (B) { f(x = all A) => (B = y); }";
        let output = bytes_set(&[(COMPOSITION_SET, source.as_bytes())]);

        assert_eq!(read_composition(&[output.items]).unwrap(), source);
    }

    #[test]
    fn read_composition_requires_exactly_one_item() {
        let two_items = bytes_set(&[("first", b"a"), ("second", b"b")]);

        assert_invalid_plan(read_composition(&[]));
        assert_invalid_plan(read_composition(&[Arc::new(vec![])]));
        assert_invalid_plan(read_composition(&[two_items.items]));
    }

    #[test]
    fn read_composition_rejects_invalid_utf8() {
        let output = bytes_set(&[(COMPOSITION_SET, &[0xff, 0xfe])]);

        assert_invalid_plan(read_composition(&[output.items]));
    }
}
