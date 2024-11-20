use daft_core::count_mode::CountMode;
use eyre::{bail, Context};
use spark_connect::expression::UnresolvedFunction;
use tracing::debug;

use crate::translation::to_daft_expr;

pub fn unresolved_to_daft_expr(f: &UnresolvedFunction) -> eyre::Result<daft_dsl::ExprRef> {
    let UnresolvedFunction {
        function_name,
        arguments,
        is_distinct,
        is_user_defined_function,
    } = f;

    let arguments: Vec<_> = arguments.iter().map(to_daft_expr).try_collect()?;

    if *is_distinct {
        bail!("Distinct not yet supported");
    }

    if *is_user_defined_function {
        bail!("User-defined functions not yet supported");
    }

    match function_name.as_str() {
        "count" => handle_count(arguments).wrap_err("Failed to handle count function"),
        n => bail!("Unresolved function {n} not yet supported"),
    }
}

pub fn handle_count(arguments: Vec<daft_dsl::ExprRef>) -> eyre::Result<daft_dsl::ExprRef> {
    let arguments: [daft_dsl::ExprRef; 1] = match arguments.try_into() {
        Ok(arguments) => arguments,
        Err(arguments) => {
            bail!("requires exactly one argument; got {arguments:?}");
        }
    };

    let [arg] = arguments;

    // special case to be consistent with how spark handles counting literals
    // see https://github.com/Eventual-Inc/Daft/issues/3421
    let count_special_case = *arg == daft_dsl::Expr::Literal(daft_dsl::LiteralValue::Int32(1));

    if count_special_case {
        debug!("special case for count");
        let result = daft_dsl::col("*").count(CountMode::All);
        return Ok(result);
    }

    let count = arg.count(CountMode::All);

    Ok(count)
}
