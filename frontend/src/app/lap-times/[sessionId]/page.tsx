'use client'

import { useEffect, useState, useMemo } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import dynamic from 'next/dynamic'
import MetricCard from '@/components/ui/MetricCard'
import type { LapTimesResponse, LapTime } from '@/types'

const RaceTraceChart = dynamic(() => import('@/components/charts/RaceTraceChart'), { ssr: false })
const PaceDistributionChart = dynamic(() => import('@/components/charts/PaceDistributionChart'), { ssr: false })
const LapTimesChart = dynamic(() => import('@/components/charts/LapTimesChart'), { ssr: false })

type SortKey = 'lap_number' | 'rider_name' | 'lap_time_ms' | 'speed'
type SortDir = 'asc' | 'desc'

function msToStr(ms: number): string {
  if (!ms || ms <= 0) return '-'
  const mins = Math.floor(ms / 60000)
  const secs = ((ms % 60000) / 1000).toFixed(3).padStart(6, '0')
  return mins > 0 ? `${mins}:${secs}` : `${secs}s`
}

export default function LapTimesPage() {
  const params = useParams()
  const sessionId = parseInt(params.sessionId as string)

  const [data, setData] = useState<LapTimesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'trace' | 'pace' | 'laps' | 'best'>('trace')
  const [sortKey, setSortKey] = useState<SortKey>('lap_number')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [filterRider, setFilterRider] = useState<string>('')

  useEffect(() => {
    setLoading(true)
    fetch(`/api/lap-times/${sessionId}`)
      .then((r) => r.json())
      .then((d: LapTimesResponse) => {
        setData(d)
        setLoading(false)
      })
      .catch(() => {
        setError('Failed to load lap times')
        setLoading(false)
      })
  }, [sessionId])

  // Derived metrics
  const kpis = useMemo(() => {
    if (!data) return null
    const { laps, bestLaps } = data
    if (!laps.length) return null

    const totalLaps = laps.length
    const fastestLap = bestLaps[0]
    const totalRiders = data.riders.length

    // Session info from first lap
    const firstLap = laps[0]
    const sessionType = firstLap.session_type
    const eventName = firstLap.event_name
    const circuitName = firstLap.circuit_name

    return { totalLaps, fastestLap, totalRiders, sessionType, eventName, circuitName }
  }, [data])

  // Sorted/filtered lap table
  const filteredLaps = useMemo(() => {
    if (!data) return []
    let rows = [...data.laps]
    if (filterRider) {
      rows = rows.filter((l) =>
        l.rider_name.toLowerCase().includes(filterRider.toLowerCase())
      )
    }
    rows.sort((a, b) => {
      let av: string | number = a[sortKey] ?? ''
      let bv: string | number = b[sortKey] ?? ''
      if (typeof av === 'string' && typeof bv === 'string') {
        return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      }
      av = Number(av)
      bv = Number(bv)
      return sortDir === 'asc' ? av - bv : bv - av
    })
    return rows
  }, [data, sortKey, sortDir, filterRider])

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <span className="text-[#444] ml-1">↕</span>
    return <span className="text-[#E8002D] ml-1">{sortDir === 'asc' ? '↑' : '↓'}</span>
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-[#E8002D]">{error}</div>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-[#888888] text-lg">Loading lap times data...</div>
      </div>
    )
  }

  if (!data || data.laps.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-64 gap-4">
        <div className="text-[#888888] text-lg">No lap times data available</div>
        <Link href="/" className="text-[#E8002D] hover:underline">← Back to Dashboard</Link>
      </div>
    )
  }

  const TABS = [
    { key: 'trace', label: 'Race Trace' },
    { key: 'pace', label: 'Pace Distribution' },
    { key: 'best', label: 'Best Laps' },
    { key: 'laps', label: 'All Laps' },
  ] as const

  return (
    <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Back */}
      <Link href="/" className="text-[#888888] hover:text-white text-sm flex items-center gap-1 mb-6">
        ← Back to Dashboard
      </Link>

      {/* Header */}
      <div className="mb-6">
        <h1 className="text-3xl font-extrabold text-white">
          Lap Times <span className="text-[#E8002D]">Analysis</span>
        </h1>
        {kpis && (
          <p className="text-[#888888] text-sm mt-1">
            {kpis.sessionType} — {kpis.eventName} ({kpis.circuitName})
          </p>
        )}
      </div>

      {/* KPI Cards */}
      {kpis && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <MetricCard
            title="Total Laps"
            value={kpis.totalLaps}
            subtitle="all riders combined"
          />
          <MetricCard
            title="Fastest Lap"
            value={kpis.fastestLap ? kpis.fastestLap.lap_time : '-'}
            subtitle={kpis.fastestLap ? kpis.fastestLap.rider_name : ''}
            accent
          />
          <MetricCard
            title="Riders"
            value={kpis.totalRiders}
            subtitle="with lap data"
          />
          <MetricCard
            title="Session"
            value={kpis.sessionType}
            subtitle={kpis.circuitName}
          />
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 mb-6 border-b border-[#222222]">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-5 py-3 text-sm font-semibold border-b-2 transition-colors ${
              activeTab === tab.key
                ? 'border-[#E8002D] text-white'
                : 'border-transparent text-[#888888] hover:text-white'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {activeTab === 'trace' && (
        <div className="bg-[#111111] border border-[#222222] rounded-xl p-5">
          <h2 className="text-white font-bold text-lg mb-4">Race Trace — Gap to Leader</h2>
          {data.raceTrace.length === 0 ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">
              Race trace not available for this session type
            </div>
          ) : (
            <RaceTraceChart raceTrace={data.raceTrace} riders={data.riders} />
          )}
        </div>
      )}

      {activeTab === 'pace' && (
        <div className="bg-[#111111] border border-[#222222] rounded-xl p-5">
          <h2 className="text-white font-bold text-lg mb-4">Pace Distribution</h2>
          <PaceDistributionChart laps={data.laps} riders={data.riders} />
        </div>
      )}

      {activeTab === 'best' && (
        <div className="bg-[#111111] border border-[#222222] rounded-xl p-5">
          <LapTimesChart bestLaps={data.bestLaps} />
        </div>
      )}

      {activeTab === 'laps' && (
        <div className="bg-[#111111] border border-[#222222] rounded-xl overflow-hidden">
          {/* Filter */}
          <div className="p-5 border-b border-[#222222] flex flex-wrap gap-3 items-center">
            <h2 className="text-white font-bold text-lg">All Laps</h2>
            <input
              type="text"
              placeholder="Filter by rider..."
              value={filterRider}
              onChange={(e) => setFilterRider(e.target.value)}
              className="ml-auto bg-[#0a0a0a] border border-[#333333] text-white rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-[#E8002D] w-48"
            />
            <span className="text-[#555555] text-sm">{filteredLaps.length} laps</span>
          </div>
          <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-[#111111] z-10">
                <tr className="border-b border-[#222222]">
                  <th
                    className="text-left text-[#888888] font-semibold px-5 py-3 cursor-pointer hover:text-white"
                    onClick={() => handleSort('lap_number')}
                  >
                    LAP <SortIcon k="lap_number" />
                  </th>
                  <th
                    className="text-left text-[#888888] font-semibold py-3 cursor-pointer hover:text-white"
                    onClick={() => handleSort('rider_name')}
                  >
                    RIDER <SortIcon k="rider_name" />
                  </th>
                  <th
                    className="text-right text-[#888888] font-semibold py-3 cursor-pointer hover:text-white"
                    onClick={() => handleSort('lap_time_ms')}
                  >
                    LAP TIME <SortIcon k="lap_time_ms" />
                  </th>
                  <th className="text-right text-[#888888] font-semibold py-3 hidden md:table-cell">T1</th>
                  <th className="text-right text-[#888888] font-semibold py-3 hidden md:table-cell">T2</th>
                  <th className="text-right text-[#888888] font-semibold py-3 hidden md:table-cell">T3</th>
                  <th className="text-right text-[#888888] font-semibold py-3 hidden lg:table-cell">T4</th>
                  <th
                    className="text-right text-[#888888] font-semibold py-3 pr-5 cursor-pointer hover:text-white hidden sm:table-cell"
                    onClick={() => handleSort('speed')}
                  >
                    SPEED <SortIcon k="speed" />
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredLaps.map((lap: LapTime, idx: number) => (
                  <tr
                    key={idx}
                    className={`border-b border-[#1a1a1a] hover:bg-[#1a1a1a] transition-colors ${
                      lap.is_best_lap ? 'bg-[#FFD700]/5' : ''
                    }`}
                  >
                    <td className="px-5 py-2 font-mono text-[#888888]">{lap.lap_number}</td>
                    <td className="py-2 text-white font-medium">{lap.rider_name}</td>
                    <td className="py-2 text-right font-mono">
                      <span
                        className={
                          lap.is_best_lap ? 'text-[#FFD700] font-bold' : 'text-[#cccccc]'
                        }
                      >
                        {lap.lap_time || msToStr(lap.lap_time_ms)}
                      </span>
                    </td>
                    <td className="py-2 text-right font-mono text-[#888888] text-xs hidden md:table-cell">
                      {lap.t1 || msToStr(lap.t1_ms || 0)}
                    </td>
                    <td className="py-2 text-right font-mono text-[#888888] text-xs hidden md:table-cell">
                      {lap.t2 || msToStr(lap.t2_ms || 0)}
                    </td>
                    <td className="py-2 text-right font-mono text-[#888888] text-xs hidden md:table-cell">
                      {lap.t3 || msToStr(lap.t3_ms || 0)}
                    </td>
                    <td className="py-2 text-right font-mono text-[#888888] text-xs hidden lg:table-cell">
                      {lap.t4 || msToStr(lap.t4_ms || 0)}
                    </td>
                    <td className="py-2 pr-5 text-right text-[#888888] text-xs hidden sm:table-cell">
                      {lap.speed ? `${lap.speed} km/h` : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
