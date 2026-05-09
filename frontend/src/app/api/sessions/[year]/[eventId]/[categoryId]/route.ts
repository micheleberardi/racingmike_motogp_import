import { NextResponse } from 'next/server'
import pool from '@/lib/db'

export async function GET(
  _request: Request,
  { params }: { params: { year: string; eventId: string; categoryId: string } }
) {
  try {
    const year = parseInt(params.year)
    const eventId = params.eventId
    const categoryId = params.categoryId

    const [rows] = await pool.execute(
      `SELECT id, type, year, event_id, category_id, category_name,
              event_name, circuit_name, analysis_url, status, session_datetime
       FROM sessions
       WHERE year = ? AND event_id = ? AND category_id = ?
       ORDER BY session_datetime ASC`,
      [year, eventId, categoryId]
    )

    return NextResponse.json(rows)
  } catch (error) {
    console.error('Error fetching sessions:', error)
    return NextResponse.json({ error: 'Failed to fetch sessions' }, { status: 500 })
  }
}
