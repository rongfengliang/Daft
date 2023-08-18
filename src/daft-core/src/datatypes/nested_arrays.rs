use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::datatypes::{BooleanArray, DaftArrayType, Field};
use crate::series::Series;
use crate::DataType;

#[derive(Clone)]
pub struct FixedSizeListArray {
    pub field: Arc<Field>,
    pub flat_child: Series,
    pub(crate) validity: Option<BooleanArray>,
}

impl DaftArrayType for FixedSizeListArray {}

impl FixedSizeListArray {
    pub fn new<F: Into<Arc<Field>>>(
        field: F,
        flat_child: Series,
        validity: Option<BooleanArray>,
    ) -> Self {
        let field: Arc<Field> = field.into();
        if !matches!(&field.as_ref().dtype, &DataType::FixedSizeList(..)) {
            panic!(
                "FixedSizeListArray::new expected FixedSizeList datatype, but received field: {}",
                field
            )
        }
        FixedSizeListArray {
            field,
            flat_child,
            validity,
        }
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 FixedSizeListArray to concat".to_string(),
            ));
        }
        let _flat_children: Vec<_> = arrays.iter().map(|a| &a.flat_child).collect();
        let _validities = arrays.iter().map(|a| &a.validity);
        let _lens = arrays.iter().map(|a| a.len());

        // TODO(FixedSizeList)
        todo!();
    }

    pub fn len(&self) -> usize {
        match &self.validity {
            None => self.flat_child.len() / self.fixed_element_len(),
            Some(validity) => validity.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn name(&self) -> &str {
        &self.field.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn child_data_type(&self) -> &DataType {
        match &self.field.dtype {
            DataType::FixedSizeList(child, _) => &child.dtype,
            _ => unreachable!("FixedSizeListArray must have DataType::FixedSizeList(..)"),
        }
    }

    pub fn rename(&self, name: &str) -> Self {
        Self::new(
            Field::new(name, self.data_type().clone()),
            self.flat_child.rename(name),
            self.validity.clone(),
        )
    }

    pub fn slice(&self, _start: usize, _end: usize) -> DaftResult<Self> {
        // TODO(FixedSizeList)
        todo!()
    }

    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        let arrow_dtype = self.data_type().to_arrow().unwrap();
        let arrow_validity = self.validity.as_ref().map(|validity| {
            arrow2::bitmap::Bitmap::from_iter(validity.into_iter().map(|v| v.unwrap()))
        });
        if self.is_empty() || self.validity.is_none() {
            Box::new(arrow2::array::FixedSizeListArray::new(
                arrow_dtype,
                self.flat_child.to_arrow(),
                arrow_validity,
            ))
        } else {
            // NOTE: Arrow2 requires that the values array is padded for values when validity is false
            // such that (values.len() / size) == self.len(). Here we naively use idx 0 as the padding value.
            let child_arrow_dtype = self.child_data_type().to_arrow().unwrap();
            let padding_series = Series::try_from((
                "",
                arrow2::array::new_null_array(child_arrow_dtype, self.fixed_element_len()),
            ))
            .unwrap();
            let take_series: Vec<Option<Series>> = (0..self.len()).map(|i| self.get(i)).collect();
            let padded_take_series: Vec<&Series> = take_series
                .iter()
                .map(|s| s.as_ref().unwrap_or(&padding_series))
                .collect();
            let dense_values = Series::concat(padded_take_series.as_slice())
                .unwrap()
                .to_arrow();
            Box::new(arrow2::array::FixedSizeListArray::new(
                arrow_dtype,
                dense_values,
                arrow_validity,
            ))
        }
    }

    pub fn fixed_element_len(&self) -> usize {
        let dtype = &self.field.as_ref().dtype;
        match dtype {
            DataType::FixedSizeList(_, s) => *s,
            _ => unreachable!("FixedSizeListArray should always have FixedSizeList datatype"),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        datatypes::{BooleanArray, Field, Int32Array},
        DataType, IntoSeries,
    };

    use super::FixedSizeListArray;

    /// Helper that returns a FixedSizeListArray, with each list element at len=3
    fn get_i32_fixed_size_list_array(validity: &[bool]) -> FixedSizeListArray {
        let field = Field::new(
            "foo",
            DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int32)), 3),
        );
        let num_valid_elements = validity.iter().map(|v| if *v { 1 } else { 0 }).sum();
        let flat_child = Int32Array::from(("foo", (0..num_valid_elements).collect::<Vec<i32>>()));
        let validity = Some(BooleanArray::from(("foo", validity)));
        FixedSizeListArray::new(field, flat_child.into_series(), validity)
    }

    #[test]
    fn test_rename() -> DaftResult<()> {
        let arr = get_i32_fixed_size_list_array(vec![true, true, false].as_slice());
        let renamed_arr = arr.rename("bar");

        assert_eq!(renamed_arr.name(), "bar");
        assert_eq!(renamed_arr.flat_child.len(), arr.flat_child.len());
        assert_eq!(
            renamed_arr
                .flat_child
                .i32()?
                .into_iter()
                .collect::<Vec<_>>(),
            arr.flat_child.i32()?.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            renamed_arr
                .validity
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            arr.validity.unwrap().into_iter().collect::<Vec<_>>()
        );
        Ok(())
    }
}
