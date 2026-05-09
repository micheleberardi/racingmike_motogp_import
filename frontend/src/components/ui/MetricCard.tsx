interface MetricCardProps {
  title: string
  value: string | number
  subtitle?: string
  accent?: boolean
}

export default function MetricCard({ title, value, subtitle, accent }: MetricCardProps) {
  return (
    <div
      className={`bg-[#111111] border rounded-xl p-5 flex flex-col gap-1 ${
        accent ? 'border-[#E8002D]/40' : 'border-[#222222]'
      }`}
    >
      <span className="text-[#888888] text-xs font-semibold uppercase tracking-widest">
        {title}
      </span>
      <span
        className={`text-3xl font-extrabold leading-tight ${
          accent ? 'text-[#E8002D]' : 'text-white'
        }`}
      >
        {value}
      </span>
      {subtitle && <span className="text-[#888888] text-sm truncate">{subtitle}</span>}
    </div>
  )
}
