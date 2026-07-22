from examples.security.schemas.assets import App, Device, DeviceType, Scanner
from examples.security.schemas.events import AppEvent, VulnEvent
from examples.security.schemas.organization import Person
from examples.security.schemas.reporting import AppAuditEvent, VulnerabilityAuditEvent
from examples.security.schemas.risk import Vuln
from structure import Transform, input, output, transform
from structure.plugin.pyspark import *


@transform(streaming=True)
class EnrichAppEvents(Transform):
    events = input(AppEvent, streaming=True)
    devices = input(Device)
    device_types = input(DeviceType)
    apps = input(App)
    scanners = input(Scanner)
    audits = output(AppAuditEvent)

    def enrich(
        self, event: AppEvent, device: Device, device_type: DeviceType, app: App, scanner: Scanner
    ) -> AppAuditEvent:
        watermark(event.occurred_at, delay="10 minutes")
        drop_duplicates(event.id)
        inner_join(device, on=device.id == event.device_id)
        inner_join(device_type, on=device_type.id == device.device_type_id)
        inner_join(app, on=app.id == event.app_id)
        inner_join(scanner, on=scanner.id == event.scanner_id)
        return AppAuditEvent(
            id=event.id,
            occurred_at=event.occurred_at,
            device_id=event.device_id,
            device_platform=device_type.platform,
            scanner_name=scanner.name,
            app_id=event.app_id,
            app_name=app.name,
            action=event.action,
            version=event.version,
        )


@transform(streaming=True)
class EnrichVulnerabilityEvents(Transform):
    events = input(VulnEvent, streaming=True)
    vulnerabilities = input(Vuln)
    devices = input(Device)
    people = input(Person)
    scanners = input(Scanner)
    audits = output(VulnerabilityAuditEvent)

    def enrich(
        self, event: VulnEvent, vuln: Vuln, device: Device, person: Person, scanner: Scanner
    ) -> VulnerabilityAuditEvent:
        watermark(event.occurred_at, delay="10 minutes")
        drop_duplicates(event.id)
        inner_join(vuln, on=vuln.id == event.vuln_id)
        inner_join(device, on=device.id == vuln.device_id)
        inner_join(person, on=person.id == vuln.owner_id)
        inner_join(scanner, on=scanner.id == event.scanner_id)
        return VulnerabilityAuditEvent(
            id=event.id,
            occurred_at=event.occurred_at,
            vuln_id=event.vuln_id,
            device_id=device.id,
            person_id=person.id,
            scanner_name=scanner.name,
            action=event.action,
            description=event.description,
            instructions=event.instructions,
        )
