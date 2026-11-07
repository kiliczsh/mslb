"""Resolve configuration once and bind normalized immutable inputs to identity."""

import hashlib
from typing import BinaryIO

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.capacity_profiles import STANDARD_REQUESTS, STRESS_REQUESTS
from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import InputError, RequestInput, ServerInput, input_digest
from mslb_contracts.runs import (
    LegacyRunSettings,
    RunSettings,
    RunSnapshot,
    ScalingSettings,
    StoredRunSettings,
    StressRunSettings,
    TemplateSettings,
    public_settings,
    stored_request_limit,
)
from mslb_engine import __version__
from mslb_engine.models import AutoscalingPolicy, Request, RunConfig, Server, ServerTemplate
from mslb_engine.simulator import Simulation
from pydantic import ValidationError


def domain_inputs(snapshot: RunSnapshot) -> tuple[tuple[Server, ...], tuple[Request, ...]]:
    return (
        tuple(
            Server(
                id=s.id,
                cpu_units_per_tick=s.cpu_units_per_tick,
                mem_mb=s.mem_mb,
                rate_limit_per_sec=s.rate_limit_per_sec,
            )
            for s in snapshot.servers
        ),
        tuple(
            Request(t=r.t, request_id=r.request_id, work_units=r.work_units, mem_mb=r.mem_mb)
            for r in snapshot.requests
        ),
    )


def domain_config(settings: StoredRunSettings) -> RunConfig:
    """Use domain validation for cross-field policy semantics on every transport."""
    scaling = settings.autoscaling
    template = scaling.server_template
    policy = AutoscalingPolicy(
        enabled=scaling.enabled,
        window_ticks=scaling.window_ticks,
        scale_out_cpu_threshold=scaling.scale_out_cpu_threshold.to_fraction(),
        scale_in_cpu_threshold=scaling.scale_in_cpu_threshold.to_fraction(),
        scale_out_drop_rate_threshold=scaling.scale_out_drop_rate_threshold.to_fraction(),
        scale_out_queue_threshold=scaling.scale_out_queue_threshold,
        cooldown_ticks=scaling.cooldown_ticks,
        min_servers=scaling.min_servers,
        max_servers=scaling.max_servers,
        scale_out_step=scaling.scale_out_step,
        scale_in_step=scaling.scale_in_step,
        server_template=ServerTemplate(
            cpu_units_per_tick=template.cpu_units_per_tick,
            mem_mb=template.mem_mb,
            rate_limit_per_sec=template.rate_limit_per_sec,
        )
        if template is not None
        else None,
    )
    return RunConfig(
        mode=public_settings(settings=settings).mode,
        strategy=settings.strategy,
        queue_timeout_ticks=settings.queue_timeout_ticks,
        max_queue_size=settings.max_queue_size,
        max_simulation_ticks=settings.max_simulation_ticks,
        max_samples=settings.max_samples,
        max_fraction_bits=settings.max_fraction_bits,
        max_metric_series=settings.max_metric_series,
        autoscaling=policy,
        max_requests=stored_request_limit(settings=settings),
    )


def settings_from_domain(config: RunConfig) -> StoredRunSettings:
    """Translate exact engine policy to strict transport settings without float conversion."""
    policy = config.autoscaling
    template = policy.server_template
    settings = RunSettings(
        mode=config.mode,
        strategy=config.strategy,
        queue_timeout_ticks=config.queue_timeout_ticks,
        max_queue_size=config.max_queue_size,
        max_simulation_ticks=config.max_simulation_ticks,
        max_samples=config.max_samples,
        max_fraction_bits=config.max_fraction_bits,
        max_metric_series=config.max_metric_series,
        autoscaling=ScalingSettings(
            enabled=policy.enabled,
            window_ticks=policy.window_ticks,
            scale_out_cpu_threshold=FractionValue.from_fraction(
                value=policy.scale_out_cpu_threshold
            ),
            scale_in_cpu_threshold=FractionValue.from_fraction(value=policy.scale_in_cpu_threshold),
            scale_out_drop_rate_threshold=FractionValue.from_fraction(
                value=policy.scale_out_drop_rate_threshold
            ),
            scale_out_queue_threshold=policy.scale_out_queue_threshold,
            cooldown_ticks=policy.cooldown_ticks,
            min_servers=policy.min_servers,
            max_servers=policy.max_servers,
            scale_out_step=policy.scale_out_step,
            scale_in_step=policy.scale_in_step,
            server_template=TemplateSettings(
                cpu_units_per_tick=template.cpu_units_per_tick,
                mem_mb=template.mem_mb,
                rate_limit_per_sec=template.rate_limit_per_sec,
            )
            if template is not None
            else None,
        ),
    )
    if config.max_requests == STANDARD_REQUESTS:
        return settings
    return StressRunSettings.model_validate(
        obj=settings.model_dump(mode="python")
        | {"capacity_profile": "stress", "max_requests": STRESS_REQUESTS}
    )


def make_snapshot(
    servers: tuple[ServerInput, ...],
    requests: tuple[RequestInput, ...],
    settings: StoredRunSettings,
) -> RunSnapshot:
    """Normalize ordering, expand defaults, validate admission, and digest semantics."""
    ordered_servers = tuple(sorted(servers, key=lambda server: server.id))
    ordered_requests = tuple(sorted(requests, key=lambda request: (request.t, request.request_id)))
    simulation = Simulation(
        servers=tuple(
            Server(
                id=s.id,
                cpu_units_per_tick=s.cpu_units_per_tick,
                mem_mb=s.mem_mb,
                rate_limit_per_sec=s.rate_limit_per_sec,
            )
            for s in ordered_servers
        ),
        requests=tuple(
            Request(t=r.t, request_id=r.request_id, work_units=r.work_units, mem_mb=r.mem_mb)
            for r in ordered_requests
        ),
        config=domain_config(settings=settings),
    )
    resolved: StoredRunSettings = settings_from_domain(config=simulation.config)
    if isinstance(settings, LegacyRunSettings):
        resolved = LegacyRunSettings.model_validate(
            obj=resolved.model_dump(mode="python") | {"mode": "sequential_compat"}
        )
    snapshot = RunSnapshot(
        engine_version=__version__,
        servers=ordered_servers,
        requests=ordered_requests,
        configuration=resolved,
        dataset_digest=input_digest(records=ordered_requests),
        server_snapshot_digest=input_digest(records=ordered_servers),
        snapshot_digest="0" * 64,
    )
    digest = hashlib.sha256(
        data=canonical_bytes(value=snapshot.model_dump(mode="json", exclude={"snapshot_digest"}))
    ).hexdigest()
    return snapshot.model_copy(update={"snapshot_digest": digest})


def verify_snapshot(snapshot: RunSnapshot) -> RunSnapshot:
    if snapshot.engine_version != __version__:
        raise InputError(field="engine_version", reason="incompatible_engine_version")
    expected = make_snapshot(
        servers=snapshot.servers, requests=snapshot.requests, settings=snapshot.configuration
    )
    if snapshot != expected:
        raise InputError(field="snapshot", reason="identity_or_normalization_mismatch")
    return snapshot


def read_snapshot(stream: BinaryIO) -> RunSnapshot:
    payload = stream.read(32 * 1024 * 1024 + 1)
    if len(payload) > 32 * 1024 * 1024:
        raise InputError(field="snapshot", reason="payload_too_large")
    try:
        snapshot = RunSnapshot.model_validate_json(json_data=payload)
    except ValidationError:
        raise InputError(field="snapshot", reason="invalid_schema") from None
    return verify_snapshot(snapshot=snapshot)
