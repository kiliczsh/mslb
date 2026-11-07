import {
  CheckCircleIcon,
  CircleNotchIcon,
  ClockCounterClockwiseIcon,
  StopCircleIcon,
  WarningCircleIcon,
  XCircleIcon,
} from "./Icons.js";

export function StatusIcon({ status }: { status: string }) {
  const Icon =
    status === "completed"
      ? CheckCircleIcon
      : status === "failed"
        ? XCircleIcon
        : status === "cancelled"
          ? StopCircleIcon
          : status === "cancel_requested"
            ? WarningCircleIcon
            : status === "queued"
              ? ClockCounterClockwiseIcon
              : CircleNotchIcon;
  return (
    <Icon size={16} className={status === "running" ? "icon-spin" : undefined} aria-hidden="true" />
  );
}
