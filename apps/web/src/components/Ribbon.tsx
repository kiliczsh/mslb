/** Decorative geometry is independent of simulation data. */
export function Ribbon({ compact = false }: { compact?: boolean }) {
  return (
    <svg
      className={compact ? "ribbon ribbon-mark" : "ribbon"}
      viewBox={compact ? "0 0 150 120" : "0 0 600 120"}
      aria-hidden="true"
      focusable="false"
    >
      <polygon fill="#dce6ff" points="0,95 42,22 85,76" />
      <polygon fill="#92b1f8" points="0,95 85,76 42,64" />
      <polygon fill="#b6ccff" points="42,22 85,76 150,91" />
      <polygon fill="#e7edff" points="85,76 150,91 192,42" />
      <polygon fill="#a7c0fc" points="150,91 192,42 238,84" />
      <polygon fill="#cbdcff" points="192,42 238,84 278,20" />
      <polygon fill="#8daef7" points="238,84 278,20 317,72" />
      <polygon fill="#dce6ff" points="278,20 317,72 384,50" />
      <polygon fill="#b6ccff" points="317,72 384,50 418,90" />
      <polygon fill="#92b1f8" points="384,50 418,90 460,31" />
      <polygon fill="#dce6ff" points="418,90 460,31 511,71" />
      <polygon fill="#b6ccff" points="460,31 511,71 558,14" />
      <polygon fill="#e7edff" points="511,71 558,14 600,57" />
    </svg>
  );
}
