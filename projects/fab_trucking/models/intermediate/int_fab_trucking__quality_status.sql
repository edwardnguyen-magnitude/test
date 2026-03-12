{{ config(materialized='view') }}

with inspections as (
    select * from {{ ref('stg_fab_trucking__inspections') }}
),

test_results as (
    select * from {{ ref('stg_fab_trucking__test_results') }}
),

defects as (
    select * from {{ ref('stg_fab_trucking__defects') }}
),

-- Aggregate inspection status per VIN
inspection_summary as (
    select
        vin,
        count(*) as total_inspections,
        sum(case when inspection_result = 'PASS' then 1 else 0 end) as passed_inspections,
        sum(case when inspection_result = 'FAIL' then 1 else 0 end) as failed_inspections,
        sum(case when inspection_result = 'REWORK_REQUIRED' then 1 else 0 end) as rework_inspections,
        max(completed_ts) as last_inspection_ts,
        max(case when inspection_result = 'FAIL' then defect_severity_max end) as max_defect_severity,
        sum(defect_count) as total_defects_found
    from inspections
    group by vin
),

-- Aggregate test status per VIN
test_summary as (
    select
        vin,
        count(*) as total_tests,
        sum(case when result = 'PASS' then 1 else 0 end) as passed_tests,
        sum(case when result = 'FAIL' then 1 else 0 end) as failed_tests,
        sum(case when result = 'INCOMPLETE' then 1 else 0 end) as incomplete_tests,
        max(end_ts) as last_test_ts
    from test_results
    group by vin
),

-- Aggregate defect status per VIN
defect_summary as (
    select
        vin,
        count(*) as total_defects,
        sum(case when resolved_ts is null then 1 else 0 end) as open_defects,
        sum(case when severity = 'CRITICAL' and resolved_ts is null then 1 else 0 end) as critical_open_defects,
        sum(case when severity = 'MAJOR' and resolved_ts is null then 1 else 0 end) as major_open_defects,
        sum(case when severity = 'MINOR' and resolved_ts is null then 1 else 0 end) as minor_open_defects,
        sum(case when detected_at_stage = 'IN_PLANT' then 1 else 0 end) as plant_defects,
        sum(case when detected_at_stage = 'DEALER_PDI' then 1 else 0 end) as dealer_defects,
        sum(case when detected_at_stage = 'WARRANTY' then 1 else 0 end) as warranty_defects
    from defects
    group by vin
),

quality_status as (
    select
        coalesce(i.vin, t.vin, d.vin) as vin,

        -- Inspection metrics
        coalesce(i.total_inspections, 0) as total_inspections,
        coalesce(i.passed_inspections, 0) as passed_inspections,
        coalesce(i.failed_inspections, 0) as failed_inspections,
        coalesce(i.rework_inspections, 0) as rework_inspections,
        i.last_inspection_ts,
        i.max_defect_severity,
        coalesce(i.total_defects_found, 0) as inspection_defects_found,

        -- Test metrics
        coalesce(t.total_tests, 0) as total_tests,
        coalesce(t.passed_tests, 0) as passed_tests,
        coalesce(t.failed_tests, 0) as failed_tests,
        coalesce(t.incomplete_tests, 0) as incomplete_tests,
        t.last_test_ts,

        -- Defect metrics
        coalesce(d.total_defects, 0) as total_defects,
        coalesce(d.open_defects, 0) as open_defects,
        coalesce(d.critical_open_defects, 0) as critical_open_defects,
        coalesce(d.major_open_defects, 0) as major_open_defects,
        coalesce(d.minor_open_defects, 0) as minor_open_defects,
        coalesce(d.plant_defects, 0) as plant_defects,
        coalesce(d.dealer_defects, 0) as dealer_defects,
        coalesce(d.warranty_defects, 0) as warranty_defects,

        -- Overall QA status
        case
            when coalesce(d.critical_open_defects, 0) > 0 then 'CRITICAL_ISSUES'
            when coalesce(d.major_open_defects, 0) > 0 then 'MAJOR_ISSUES'
            when coalesce(i.failed_inspections, 0) > 0 or coalesce(t.failed_tests, 0) > 0 then 'NEEDS_REWORK'
            when coalesce(i.rework_inspections, 0) > 0 or coalesce(t.incomplete_tests, 0) > 0 then 'IN_PROGRESS'
            when coalesce(i.total_inspections, 0) = 0 and coalesce(t.total_tests, 0) = 0 then 'PENDING'
            else 'COMPLETE'
        end as qa_status,

        -- Test status
        case
            when coalesce(t.failed_tests, 0) > 0 then 'FAIL'
            when coalesce(t.incomplete_tests, 0) > 0 then 'PENDING'
            when coalesce(t.total_tests, 0) = 0 then 'NOT_REQUIRED'
            else 'PASS'
        end as test_status

    from inspection_summary i
    full outer join test_summary t on i.vin = t.vin
    full outer join defect_summary d on coalesce(i.vin, t.vin) = d.vin
)

select * from quality_status
