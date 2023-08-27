select s.month,
        count(*)                                 as total,
       sum(s.has_async::integer)                as has_async,
       sum(s.has_async_comp::integer)           as has_async_comp,
       sum(s.has_fstring::integer)              as has_fstring,
       sum(s.has_annotations::integer)          as has_annotations,
       sum(s.has_try_star::integer)             as has_try_star,
       sum(s.has_match::integer)                as has_match,
       sum(s.has_walrus::integer)               as has_walrus,
       sum(s.has_dataclasses::integer)          as has_dataclasses,
       sum(s.has_generator_expression::integer) as has_generator_expression,
       sum(s.has_list_comp::integer)            as has_list_comp,
       sum(s.has_dict_comp::integer)            as has_dict_comp,
       sum(s.has_set_comp::integer)             as has_set_comp,
from (select project_name, date_trunc('month', uploaded_on) as month, max (stats.has_async) as has_async, max (stats.has_async_comp) as has_async_comp, max (stats.has_fstring) as has_fstring, max (stats.has_modulo_formatting) as has_modulo_formatting, max (stats.has_annotations) as has_annotations, max (stats.has_try_star) as has_try_star, max (stats.has_match) as has_match, max (stats.has_walrus) as has_walrus, max (stats.has_dataclasses) as has_dataclasses, max (stats.has_generator_expression) as has_generator_expression, max (stats.has_list_comp) as has_list_comp, max (stats.has_dict_comp) as has_dict_comp, max (stats.has_set_comp) as has_set_comp, from 'data/dataset/*.parquet' p
    inner join 'data/omg/*.parquet' d
    ON (p.hash = d.hash)
    group by 1, 2) s
where s.month > '2015-01-01'
group by 1
order by 1 desc
offset 1