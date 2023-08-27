use datafusion::arrow::array::{ArrayBuilder, BooleanBuilder, StructArray, StructBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use itertools::Itertools;

#[derive(Debug, Default)]
pub struct Stats {
    pub has_async: bool,
    pub has_async_comp: bool,
    pub has_fstring: bool,
    pub has_modulo_formatting: bool,
    pub has_annotations: bool,
    pub has_try_star: bool,
    pub has_match: bool,
    pub has_walrus: bool,
    pub has_matrix_multiply: bool,
    pub has_dataclasses: bool,
    pub has_generator_expression: bool,
    pub has_list_comp: bool,
    pub has_dict_comp: bool,
    pub has_set_comp: bool,
}

impl Stats {
    pub fn into_array(self) -> [bool; 14] {
        [
            self.has_async,
            self.has_async_comp,
            self.has_fstring,
            self.has_modulo_formatting,
            self.has_annotations,
            self.has_try_star,
            self.has_match,
            self.has_walrus,
            self.has_matrix_multiply,
            self.has_dataclasses,
            self.has_generator_expression,
            self.has_list_comp,
            self.has_dict_comp,
            self.has_set_comp
        ]
    }

    pub fn arrow_fields() -> Vec<Field> {
        vec![
            Field::new("has_async", DataType::Boolean, false),
            Field::new("has_async_comp", DataType::Boolean, false),
            Field::new("has_fstring", DataType::Boolean, false),
            Field::new("has_modulo_formatting", DataType::Boolean, false),
            Field::new("has_annotations", DataType::Boolean, false),
            Field::new("has_try_star", DataType::Boolean, false),
            Field::new("has_match", DataType::Boolean, false),
            Field::new("has_walrus", DataType::Boolean, false),
            Field::new("has_matrix_multiply", DataType::Boolean, false),
            Field::new("has_dataclasses", DataType::Boolean, false),
            Field::new("has_generator_expression", DataType::Boolean, false),
            Field::new("has_list_comp", DataType::Boolean, false),
            Field::new("has_dict_comp", DataType::Boolean, false),
            Field::new("has_set_comp", DataType::Boolean, false),
        ]
    }

    pub fn field_builders(capacity: usize) -> [BooleanBuilder; 14] {
        [
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
            BooleanBuilder::with_capacity(capacity),
        ]
    }
}

pub trait ToStructArray {
    fn to_struct_array(self) -> datafusion::arrow::array::StructArray;
}

impl ToStructArray for Vec<Option<Stats>> {
    fn to_struct_array(self) -> StructArray {
        let fields = Stats::arrow_fields();
        let fields_len = fields.len();
        let mut field_builders = Stats::field_builders(self.len());
        let total_items = self.len();

        for item in self {
            match item {
                Some(v) => {
                    let array = v.into_array();
                    for i in 0..fields_len {
                        field_builders[i].append_value(array[i]);
                    }
                }
                None => {
                    for i in 0..fields_len {
                        field_builders[i].append_value(false);
                    }
                }
            }
        }
        let mut builder = StructBuilder::new(
            fields,
            field_builders
                .into_iter()
                .map(|b| Box::new(b) as Box<dyn ArrayBuilder>)
                .collect_vec(),
        );

        for _ in 0..total_items {
            builder.append(true);
        }

        builder.finish()
    }
}
