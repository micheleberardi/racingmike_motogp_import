'use client'

import { useEffect, useState } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import ReactECharts from 'echarts-for-react'
import SessionSelector from '@/components/ui/SessionSelector'
import { apiUrl } from '@/lib/api'
import type { Session, Result, Event } from '@/types'

function getPositionColor(pos: number | null) {
  if (pos === 1) return '#FFD700'
  if (pos === 2) return '#C0C0C0'
  if (pos === 3) return '#CD7F32'
  return '#555555'
}

function formatDate(dateStr: string) {
  if (!dateStr) return ''
  const d = new Date(dateStr)
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })
}

function formatLocalDate(dateStr: string, timezone: string) {
  if (!dateStr || !timezone) return ''
  try {
    const d = new Date(dateStr)
    return d.toLocaleDateString('en-GB', {
      day: 'numeric', month: 'long', year: 'numeric',
      timeZone: timezone,
    })
  } catch {
    return formatDate(dateStr)
  }
}

function formatLocalTime(dateStr: string, timezone: string) {
  if (!dateStr || !timezone) return ''
  try {
    const d = new Date(dateStr)
    return d.toLocaleTimeString('en-GB', {
      hour: '2-digit', minute: '2-digit',
      timeZone: timezone,
      timeZoneName: 'short',
    })
  } catch {
    return ''
  }
}

export default function RacePage() {
  const params = useParams()
  const eventId = params.eventId as string

  const [event, setEvent] = useState<Event | null>(null)
  const [sessions, setSessions] = useState<Session[]>([])
  const [selectedSession, setSelectedSession] = useState<Session | null>(null)
  const [results, setResults] = useState<Result[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Fetch event info from all years
  useEffect(() => {
    const fetchEvent = async () => {
      try {
        const yearsRes = await fetch(apiUrl('/api/years'))
        const years: number[] = await yearsRes.json()
        for (const year of years) {
          const eventsRes = await fetch(apiUrl(`/api/events/${year}`))
          const events: Event[] = await eventsRes.json()
          const found = events.find((e) => e.id === eventId)
          if (found) {
            setEvent(found)
            return { event: found, year }
          }
        }
      } catch {
        setError('Failed to find event')
      }
      return null
    }

    fetchEvent().then((result) => {
      if (!result) return
      const { event: foundEvent, year } = result
      // Get categories for this year
      fetch(apiUrl(`/api/categories/${year}`))
        .then((r) => r.json())
        .then((cats) => {
          const motogp = cats.find((c: { name: string }) =>
            c.name.toLowerCase().includes('motogp')
          )
          const cat = motogp || cats[0]
          if (!cat) return
          return fetch(apiUrl(`/api/sessions/${year}/${foundEvent.id}/${cat.id}`))
        })
        .then((r) => r?.json())
        .then((sess: Session[]) => {
          if (!sess) return
          setSessions(sess)
          // Auto-select race session, or last completed
          const race = sess.find((s) => s.type === 'RAC' && s.status?.toUpperCase() === 'FINISHED')
          const lastCompleted = [...sess]
            .reverse()
            .find((s) => s.status?.toUpperCase() === 'FINISHED')
          setSelectedSession(race || lastCompleted || sess[sess.length - 1] || null)
          setLoading(false)
        })
        .catch(() => {
          setLoading(false)
        })
    })
  }, [eventId])

  useEffect(() => {
    if (!selectedSession) return
    setLoading(true)
    fetch(apiUrl(`/api/results/${selectedSession.id}`))
      .then((r) => r.json())
      .then((data: Result[]) => {
        setResults(Array.isArray(data) ? data : [])
        setLoading(false)
      })
      .catch(() => {
        setLoading(false)
      })
  }, [selectedSession])

  // Top speed chart
  const topSpeedData = results
    .filter((r) => r.top_speed && r.top_speed > 0 && r.position !== null)
    .sort((a, b) => (b.top_speed || 0) - (a.top_speed || 0))
    .slice(0, 15)

  const speedChartOption = {
    backgroundColor: '#111111',
    grid: { top: 10, right: 80, bottom: 10, left: 160, containLabel: false },
    xAxis: {
      type: 'value',
      min: topSpeedData.length > 0 ? Math.min(...topSpeedData.map((r) => r.top_speed)) - 5 : 0,
      axisLabel: {
        color: '#888888',
        fontSize: 11,
        formatter: (v: number) => `${v} km/h`,
      },
      axisLine: { lineStyle: { color: '#333333' } },
      splitLine: { lineStyle: { color: '#1a1a1a' } },
    },
    yAxis: {
      type: 'category',
      data: topSpeedData.map((r) => r.rider_full_name),
      inverse: true,
      axisLabel: { color: '#cccccc', fontSize: 11 },
      axisLine: { lineStyle: { color: '#333333' } },
    },
    series: [
      {
        type: 'bar',
        data: topSpeedData.map((r, i) => ({
          value: r.top_speed,
          itemStyle: {
            color: i === 0 ? '#E8002D' : r.team_color || '#333333',
          },
        })),
        label: {
          show: true,
          position: 'right',
          color: '#cccccc',
          fontSize: 11,
          formatter: (p: { value: number }) => `${p.value} km/h`,
        },
        barMaxWidth: 24,
      },
    ],
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1a1a1a',
      borderColor: '#333333',
      textStyle: { color: '#fff', fontSize: 12 },
    },
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-[#E8002D]">{error}</div>
      </div>
    )
  }

  return (
    <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Back link */}
      <Link href="/" className="text-[#888888] hover:text-white text-sm flex items-center gap-1 mb-6">
        ← Back to Dashboard
      </Link>

      {/* Event header */}
      <div className="bg-[#111111] border border-[#222222] rounded-xl p-6 mb-6">
        {event ? (
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h1 className="text-3xl font-extrabold text-white">{event.name}</h1>
              <p className="text-[#E8002D] font-semibold mt-1">{event.circuit_name}</p>
              <p className="text-[#888888] text-sm mt-1">
                {event.country_name} — {formatDate(event.date_start)} to {formatDate(event.date_end)}
              </p>
              {event.timezone && event.timezone !== 'UTC' && (
                <p className="text-[#555555] text-xs mt-1">
                  🕐 Local timezone: <span className="text-[#888888]">{event.timezone}</span>
                  {' · '}Race week: {formatLocalDate(event.date_start, event.timezone)}
                  {' – '}{formatLocalDate(event.date_end, event.timezone)}
                </p>
              )}
            </div>
          </div>
        ) : (
          <div className="h-16 bg-[#1a1a1a] rounded animate-pulse" />
        )}
      </div>

      {/* Session tabs */}
      {sessions.length > 0 && (
        <div className="mb-6">
          <SessionSelector
            sessions={sessions}
            selectedId={selectedSession?.id || null}
            onSelect={(id) => {
              const s = sessions.find((sess) => sess.id === id)
              if (s) setSelectedSession(s)
            }}
          />
        </div>
      )}

      {/* Session info + lap times link */}
      {selectedSession && (
        <div className="flex items-center justify-between mb-4">
          <div>
            <span className="text-white font-bold">{selectedSession.type}</span>
            <span className="text-[#888888] text-sm ml-2">
              {selectedSession.event_name} — {selectedSession.category_name}
            </span>
            {selectedSession.session_datetime && event?.timezone && (
              <span className="text-[#555555] text-xs ml-3">
                {formatLocalTime(selectedSession.session_datetime, event.timezone)}
              </span>
            )}
          </div>
          <Link
            href={`/lap-times/${selectedSession.id}`}
            className="px-4 py-2 bg-[#E8002D] text-white rounded-lg text-sm font-semibold hover:bg-[#c0001f] transition-colors"
          >
            Lap Times Analysis →
          </Link>
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-5 gap-6">
        {/* Results Table */}
        <div className="xl:col-span-3 bg-[#111111] border border-[#222222] rounded-xl overflow-hidden">
          <div className="p-5 border-b border-[#222222]">
            <h2 className="text-white font-bold text-lg">Results</h2>
          </div>
          {loading ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">Loading...</div>
          ) : results.length === 0 ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">
              No results available for this session
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[#222222]">
                    <th className="text-left text-[#888888] font-semibold px-5 py-3 w-12">POS</th>
                    <th className="text-left text-[#888888] font-semibold px-2 py-3 w-8">#</th>
                    <th className="text-left text-[#888888] font-semibold py-3">RIDER</th>
                    <th className="text-left text-[#888888] font-semibold py-3 hidden md:table-cell">TEAM</th>
                    <th className="text-left text-[#888888] font-semibold py-3 hidden lg:table-cell">BIKE</th>
                    <th className="text-right text-[#888888] font-semibold py-3">PTS</th>
                    <th className="text-right text-[#888888] font-semibold py-3 hidden sm:table-cell pr-5">GAP</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((result, idx) => {
                    const isDnf = result.position === null
                    const rowBg = result.team_color
                      ? `${result.team_color}12`
                      : 'transparent'
                    const rowHover = result.team_color
                      ? `${result.team_color}22`
                      : '#1a1a1a'
                    return (
                      <tr
                        key={idx}
                        className="border-b border-[#1a1a1a] transition-colors"
                        style={{ backgroundColor: rowBg }}
                        onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = rowHover)}
                        onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = rowBg)}
                      >
                        <td className="px-5 py-3">
                          <span
                            className="inline-flex items-center justify-center w-7 h-7 rounded font-bold text-sm"
                            style={{
                              background: getPositionColor(result.position) + '33',
                              color: isDnf ? '#555555' : getPositionColor(result.position),
                            }}
                          >
                            {result.position ?? 'DNF'}
                          </span>
                        </td>
                        <td className="px-2 py-3 text-[#555555] font-mono text-xs">
                          {result.rider_number}
                        </td>
                        <td className="py-3">
                          <div
                            style={{
                              borderLeft: result.team_color
                                ? `3px solid ${result.team_color}`
                                : '3px solid #333333',
                              paddingLeft: 10,
                            }}
                          >
                            <div className={`font-medium ${isDnf ? 'text-[#666666]' : 'text-white'}`}>
                              {result.rider_full_name}
                            </div>
                          </div>
                        </td>
                        <td className={`py-3 text-xs hidden md:table-cell ${isDnf ? 'text-[#555555]' : 'text-[#888888]'}`}>
                          {result.team_name}
                        </td>
                        <td className={`py-3 text-xs hidden lg:table-cell ${isDnf ? 'text-[#555555]' : 'text-[#888888]'}`}>
                          {result.constructor_name}
                        </td>
                        <td className="py-3 text-right font-bold text-[#E8002D]">
                          {result.points || 0}
                        </td>
                        <td className="py-3 pr-5 text-right text-xs hidden sm:table-cell" style={{ color: isDnf ? '#555555' : '#888888' }}>
                          {isDnf ? 'DNF' : (result.gap_first || '-')}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Top Speed Chart */}
        <div className="xl:col-span-2 bg-[#111111] border border-[#222222] rounded-xl p-5">
          <h2 className="text-white font-bold text-lg mb-4">Top Speed</h2>
          {loading ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">Loading...</div>
          ) : topSpeedData.length === 0 ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">
              No speed data available
            </div>
          ) : (
            <ReactECharts
              option={speedChartOption}
              style={{
                height: `${Math.max(200, topSpeedData.length * 28 + 40)}px`,
                width: '100%',
              }}
              opts={{ renderer: 'canvas' }}
            />
          )}
        </div>
      </div>
    </div>
  )
}
