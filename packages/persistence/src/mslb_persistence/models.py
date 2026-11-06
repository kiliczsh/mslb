"""Compatibility exports and complete ORM metadata registration for migrations."""

from mslb_persistence.catalog_models import Dataset as Dataset
from mslb_persistence.catalog_models import Installation as Installation
from mslb_persistence.catalog_models import ManagedServer as ManagedServer
from mslb_persistence.catalog_models import ServerHistory as ServerHistory
from mslb_persistence.docker_models import DockerControlSlot as DockerControlSlot
from mslb_persistence.docker_models import DockerDeployment as DockerDeployment
from mslb_persistence.docker_models import DockerPublication as DockerPublication
from mslb_persistence.docker_models import DockerResource as DockerResource
from mslb_persistence.docker_models import DockerRunState as DockerRunState
from mslb_persistence.job_models import Attempt as Attempt
from mslb_persistence.job_models import ExecutionSlot as ExecutionSlot
from mslb_persistence.job_models import IdempotencyRecord as IdempotencyRecord
from mslb_persistence.job_models import Job as Job
from mslb_persistence.job_models import Outbox as Outbox
from mslb_persistence.job_models import Run as Run
from mslb_persistence.orm_base import Base as Base
from mslb_persistence.output_models import CommittedChunk as CommittedChunk
from mslb_persistence.output_models import IndexedEvent as IndexedEvent
from mslb_persistence.output_models import OutputProgress as OutputProgress
from mslb_persistence.output_models import PublishedArtifact as PublishedArtifact
from mslb_persistence.output_models import SavedCheckpoint as SavedCheckpoint
from mslb_persistence.resource_models import ResourceEvidence as ResourceEvidence
