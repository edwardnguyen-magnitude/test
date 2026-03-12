with audit_log as (

    select * from {{ ref('stg_claim_audit_log') }}

),

-- get the final action per claim (highest audit_id = latest action)
final_actions as (

    select
        claim_id,
        new_status as final_status,
        action as final_action,
        performed_by as resolved_by
    from audit_log
    where (claim_id, audit_id) in (
        select claim_id, max(audit_id)
        from audit_log
        group by claim_id
    )

),

lifecycle as (

    select
        a.claim_id,

        -- key timestamps
        min(case when a.action = 'Submitted' then a.action_date end) as submitted_at,
        min(case when a.action = 'Routed' then a.action_date end) as routed_at,
        min(case when a.action in ('Approved', 'Rejected') then a.action_date end) as resolved_at,

        -- final outcome
        f.final_status,
        f.final_action,
        f.resolved_by,

        -- workflow metrics
        count(*) as total_actions,

        -- time metrics (in hours)
        extract(epoch from
            min(case when a.action = 'Routed' then a.action_date end)
            - min(case when a.action = 'Submitted' then a.action_date end)
        ) / 3600.0 as hours_to_route,

        extract(epoch from
            min(case when a.action in ('Approved', 'Rejected') then a.action_date end)
            - min(case when a.action = 'Submitted' then a.action_date end)
        ) / 3600.0 as hours_to_resolve,

        -- routing info
        bool_or(a.routing_rule = 'LOW_RISK_AUTO_APPROVE') as was_auto_routed,

        -- reviewer info (from Routed action notes)
        max(case when a.action = 'Routed' then a.performed_by end) as routed_by

    from audit_log a
    inner join final_actions f on a.claim_id = f.claim_id
    group by a.claim_id, f.final_status, f.final_action, f.resolved_by

)

select * from lifecycle
