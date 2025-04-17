{%- macro combine_property_data() -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')()) }}
{%- endmacro -%}

{% macro default__combine_property_data() %}
    {# Check if both start_date and end_date are available #}
    {%- if var('start_date', none) is not none and var('end_date', none) is not none -%}
        {%- set earliest_shard_to_retrieve = var('start_date')|int -%}
        {%- set latest_shard_to_retrieve = var('end_date')|int -%}
    {% else %}
        {# Fallback to incremental or start_date behavior #}
        {% if not should_full_refresh() %}
            {%- set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int -%}
        {% else %}
            {%- set earliest_shard_to_retrieve = var('start_date')|int -%}
        {% endif %}
        {%- set latest_shard_to_retrieve = modules.datetime.date.today()|string|replace("-", "")|int -%}
    {% endif %}

    {% if execute %}
        {{ log("Starting clone operation for date range: " ~ earliest_shard_to_retrieve ~ " to " ~ latest_shard_to_retrieve, True) }}
    {% endif %}

    {% for property_id in var('property_ids') %}
        {%- set schema_name = "analytics_" + property_id|string -%}

        {% if execute %}
            {{ log("Processing property ID: " ~ property_id, True) }}
        {% endif %}

        {%- set combine_specified_property_data_query -%}
            create schema if not exists `{{target.project}}.{{var('combined_dataset')}}`;

            {# Copy intraday tables #}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_intraday_%', database=var('source_project')) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_intraday_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int and relation_suffix|int <= latest_shard_to_retrieve|int -%}
                    {% if execute %}
                        {{ log("Cloning intraday table: " ~ relation.identifier ~ " for property " ~ property_id, True) }}
                    {% endif %}
                    create or replace table `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}` clone `{{var('source_project')}}.analytics_{{property_id}}.events_intraday_{{relation_suffix}}`;
                {%- endif -%}
            {% endfor %}

            {# Copy daily tables and drop old intraday table #}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_%', exclude='events_intraday_%', database=var('source_project')) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int and relation_suffix|int <= latest_shard_to_retrieve|int -%}
                    {% if execute %}
                        {{ log("Cloning daily table: " ~ relation.identifier ~ " for property " ~ property_id, True) }}
                    {% endif %}
                    create or replace table `{{target.project}}.{{var('combined_dataset')}}.events_{{relation_suffix}}{{property_id}}` clone `{{var('source_project')}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
                    {% if execute %}
                        {{ log("Dropping corresponding intraday table for " ~ relation_suffix ~ " (if exists)", True) }}
                    {% endif %}
                    drop table if exists `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}`;
                {%- endif -%}
            {% endfor %}
        {%- endset -%}

        {% do run_query(combine_specified_property_data_query) %}

        {% if execute %}
            {{ log("Completed processing for property ID: " ~ property_id, True) }}
        {% endif %}
    {% endfor %}

    {% if execute %}
        {{ log("Clone operation completed for all properties", True) }}
    {% endif %}
{% endmacro %}
