interface RiderBadgeProps {
  position: number | null
  riderName: string
  riderNumber?: string
  teamColor?: string
  compact?: boolean
}

export default function RiderBadge({
  position,
  riderName,
  riderNumber,
  teamColor,
  compact,
}: RiderBadgeProps) {
  const posColor =
    position === 1
      ? '#FFD700'
      : position === 2
      ? '#C0C0C0'
      : position === 3
      ? '#CD7F32'
      : '#555555'

  return (
    <div className={`flex items-center gap-2 ${compact ? '' : 'py-1'}`}>
      <span
        className={`inline-flex items-center justify-center rounded font-bold text-xs shrink-0 ${
          compact ? 'w-5 h-5' : 'w-7 h-7'
        }`}
        style={{
          background: posColor + '22',
          color: posColor,
          border: `1px solid ${posColor}44`,
        }}
      >
        {position ?? '-'}
      </span>
      {riderNumber && (
        <span className="text-[#555555] text-xs font-mono w-5 text-right shrink-0">
          {riderNumber}
        </span>
      )}
      <span
        className={`font-medium truncate ${compact ? 'text-sm' : 'text-sm'}`}
        style={{ borderLeft: teamColor ? `3px solid ${teamColor}` : undefined, paddingLeft: teamColor ? 8 : 0 }}
      >
        {riderName}
      </span>
    </div>
  )
}
