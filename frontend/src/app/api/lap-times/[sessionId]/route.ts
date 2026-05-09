import { NextResponse } from 'next/server'
import pool from '@/lib/db'
import type { LapTime, RaceTracePoint, BestLap } from '@/types'

function median(values: number[]): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

export async function GET(
  _request: Request,
  { params }: { params: { sessionId: string } }
) {
  try {
    const sessionId = parseInt(params.sessionId)

    const [rows] = await pool.execute(
      `SELECT session_id, rider_name, rider_number, lap_number,
              lap_time, lap_time_ms, t1, t1_ms, t2, t2_ms,
              t3, t3_ms, t4, t4_ms, speed, is_best_lap,
              year, event_id, category_id, event_name, circuit_name, session_type
       FROM lap_times
       WHERE session_id = ?
       ORDER BY rider_name ASC, lap_number ASC`,
      [sessionId]
    )

    const laps = rows as LapTime[]

    // Compute per-rider median lap time for outlier filtering
    const riderLapTimes = new Map<string, number[]>()
    for (const lap of laps) {
      if (lap.lap_time_ms && lap.lap_time_ms > 0) {
        if (!riderLapTimes.has(lap.rider_name)) {
          riderLapTimes.set(lap.rider_name, [])
        }
        riderLapTimes.get(lap.rider_name)!.push(lap.lap_time_ms)
      }
    }

    const riderMedians = new Map<string, number>()
    for (const [rider, times] of riderLapTimes.entries()) {
      riderMedians.set(rider, median(times))
    }

    // Filter outlier laps (> 115% of rider's median)
    const filteredLaps = laps.filter((lap) => {
      if (!lap.lap_time_ms || lap.lap_time_ms <= 0) return false
      const med = riderMedians.get(lap.rider_name) || 0
      return med === 0 || lap.lap_time_ms <= med * 1.15
    })

    // Compute cumulative times per rider per lap
    const riderCumulative = new Map<string, number>()
    const cumulativeData = new Map<string, Map<number, number>>() // rider -> lap -> cumulative_ms

    // Sort filtered laps by rider, lap_number
    const sorted = [...filteredLaps].sort((a, b) => {
      if (a.rider_name < b.rider_name) return -1
      if (a.rider_name > b.rider_name) return 1
      return a.lap_number - b.lap_number
    })

    for (const lap of sorted) {
      if (!cumulativeData.has(lap.rider_name)) {
        cumulativeData.set(lap.rider_name, new Map())
        riderCumulative.set(lap.rider_name, 0)
      }
      const prev = riderCumulative.get(lap.rider_name) || 0
      const cumul = prev + lap.lap_time_ms
      riderCumulative.set(lap.rider_name, cumul)
      cumulativeData.get(lap.rider_name)!.set(lap.lap_number, cumul)
    }

    // Find min cumulative (leader) per lap number
    const allLapNumbers = new Set<number>()
    for (const [, lapMap] of cumulativeData.entries()) {
      for (const lapNum of lapMap.keys()) {
        allLapNumbers.add(lapNum)
      }
    }

    const leaderPerLap = new Map<number, number>()
    for (const lapNum of allLapNumbers) {
      let minCumul = Infinity
      for (const [, lapMap] of cumulativeData.entries()) {
        const c = lapMap.get(lapNum)
        if (c !== undefined && c < minCumul) {
          minCumul = c
        }
      }
      if (isFinite(minCumul)) {
        leaderPerLap.set(lapNum, minCumul)
      }
    }

    // Build race trace
    const raceTrace: RaceTracePoint[] = []
    for (const [rider, lapMap] of cumulativeData.entries()) {
      for (const [lapNum, cumul] of lapMap.entries()) {
        const leaderCumul = leaderPerLap.get(lapNum)
        if (leaderCumul !== undefined) {
          const gap_s = (cumul - leaderCumul) / 1000
          raceTrace.push({ rider_name: rider, lap_number: lapNum, gap_s })
        }
      }
    }

    // Compute best laps per rider
    const riderBestLaps = new Map<string, LapTime>()
    for (const lap of laps) {
      if (!lap.lap_time_ms || lap.lap_time_ms <= 0) continue
      const current = riderBestLaps.get(lap.rider_name)
      if (!current || lap.lap_time_ms < current.lap_time_ms) {
        riderBestLaps.set(lap.rider_name, lap)
      }
    }

    const bestLaps: BestLap[] = Array.from(riderBestLaps.values())
      .map((lap) => ({
        rider_name: lap.rider_name,
        lap_time: lap.lap_time,
        lap_time_ms: lap.lap_time_ms,
        t1_ms: lap.t1_ms,
        t2_ms: lap.t2_ms,
        t3_ms: lap.t3_ms,
        t4_ms: lap.t4_ms,
        speed: lap.speed,
      }))
      .sort((a, b) => a.lap_time_ms - b.lap_time_ms)

    const riders = Array.from(cumulativeData.keys()).sort()

    return NextResponse.json({ laps, raceTrace, bestLaps, riders })
  } catch (error) {
    console.error('Error fetching lap times:', error)
    return NextResponse.json({ error: 'Failed to fetch lap times' }, { status: 500 })
  }
}
