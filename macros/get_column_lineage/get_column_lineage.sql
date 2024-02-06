-- There are multiple macros defined in this script.

{#
Run this command:
    dbt run-operation get_column_lineage_models --args '{model: stg_third, column: fruit}'
#}

{% macro get_column_lineage_models(model, column) -%}
{# This macro returns the list of downstream models that reference a specified column. #}

    {%- set dep_list = get_list_of_refs(model) -%}
    {%- set results = [] %}
    {%- set model_list = [] %}

    {%- if dep_list|length > 0 -%}
    -- check immediate dependencies
        {% for item in dep_list %} -- follow down a path
            {{ results.append(does_ref_contain_column(originating_model=model, model=item, column=column))|default("", true) }}
            {% if results[0] %}
                {{ model_list.append(item)|default("", true) }}
            {% endif %}
        {% endfor %}
    {%- endif -%}

    -- At this point, we know which immediate downstream models have the column in dep_list
    

{{ model_list[0] }}

{%- endmacro %}


{%- macro get_list_of_refs(model) -%}
{# This macro returns a list of models that reference the model input. #}

    {%- set dep_list = [] -%}
    {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") -%}
        {%- for deps in node.depends_on.nodes -%}
            {%- if model in deps -%}
                {{ dep_list.append(node.unique_id.split('.')[2])|default("", true) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {{ return(dep_list) }}

{%- endmacro -%}

{%- macro does_ref_contain_column(originating_model, model, column) -%}
{# This macro returns an array with the values [true / false, renamed_column or original column / '']. #}

    {%- set raw_code_list = [] -%}
    {%- for node in graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') -%}
        {%- if model in node.unique_id -%}
            {{ raw_code_list.append(node.raw_code.lower())|default("", true) }}
        {%- endif -%}
    {%- endfor -%}

    {% set sql_segment = raw_code_list[0].split(originating_model)[0].split('select')[-1] %}

    {% if column in sql_segment %}
        {% set is_column_in_sql, new_column_name = true, sql_segment.split(column)[-1].split(',')[0]|trim %}
    {% else %}
        {% set is_column_in_sql, new_column_name = false, '' %}
    {% endif %}

    {{ return([is_column_in_sql, new_column_name]) }}

{%- endmacro -%}