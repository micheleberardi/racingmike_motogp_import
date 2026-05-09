import { NextResponse } from 'next/server'
import pool from '@/lib/db'

export async function GET(
  _request: Request,
  { params }: { params: { sessionId: string } }
) {
  try {
    const sessionId = parseInt(params.sessionId)

    const [rows] = await pool.execute(
      `SELECT r.session_id, r.rider_id, r.rider_full_name, r.rider_number,
              r.team_name, r.constructor_name, r.position, r.points,
              r.total_laps, r.time, r.gap_first, r.top_speed, r.average_speed,
              r.year, r.event_id, r.category_id, r.session_type,
              tr.team_color
       FROM results r
       LEFT JOIN TeamRiders tr
         ON tr.year = r.year
         AND tr.category_id = r.category_id
         AND tr.rider_full_name = r.rider_full_name
       WHERE r.session_id = ?
       ORDER BY r.position ASC`,
      [sessionId]
    )

    return NextResponse.json(rows)
  } catch (error) {
    console.error('Error fetching results:', error)
    return NextResponse.json({ error: 'Failed to fetch results' }, { status: 500 })
  }
}
