use crate::ExprRef;
use common_error::DaftError;
use daft_core::datatypes::DataType;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::functions::FunctionExpr;
use common_error::DaftResult;

use super::super::FunctionEvaluator;

pub struct HeightEvaluator {}

impl FunctionEvaluator for HeightEvaluator {
    fn fn_name(&self) -> &'static str {
        "height"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        _expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;

                // Validate the input field type
                match &input_field.dtype {
                    DataType::Image(_) | DataType::FixedShapeImage(..) => {
                        Ok(Field::new(input_field.name.clone(), DataType::UInt32))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Image height can only height ImageArrays and FixedShapeImage, got {}",
                        input_field
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input] => input.image_height(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
