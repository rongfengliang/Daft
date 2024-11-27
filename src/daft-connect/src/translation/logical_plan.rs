use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, Context};
use spark_connect::{relation::RelType, Limit, Relation};
use tracing::warn;

use crate::translation::logical_plan::{
    aggregate::aggregate, project::project, range::range, read::read,
};

mod aggregate;
mod project;
mod range;
mod read;

pub async fn to_logical_plan(relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
    if let Some(common) = relation.common {
        warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Limit(l) => limit(*l)
            .await
            .wrap_err("Failed to apply limit to logical plan"),
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        RelType::Project(p) => project(*p)
            .await
            .wrap_err("Failed to apply project to logical plan"),
        RelType::Aggregate(a) => aggregate(*a)
            .await
            .wrap_err("Failed to apply aggregate to logical plan"),
        RelType::Read(r) => read(r)
            .await
            .wrap_err("Failed to apply table read to logical plan"),
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}

async fn limit(limit: Limit) -> eyre::Result<LogicalPlanBuilder> {
    let Limit { input, limit } = limit;

    let Some(input) = input else {
        bail!("input must be set");
    };

    let plan = Box::pin(to_logical_plan(*input))
        .await?
        .limit(i64::from(limit), false)?; // todo: eager or no

    Ok(plan)
}
