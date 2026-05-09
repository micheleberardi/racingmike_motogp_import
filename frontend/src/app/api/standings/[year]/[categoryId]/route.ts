import { NextResponse } from 'next/server'
import pool from '@/lib/db'

export async function GET(
  _request: Request,
  { params }: { params: { year: string; categoryId: string } }
) {
  try {
    const year = parseInt(params.year)
    const categoryId = parseInt(params.categoryId)

    const [rows] = await pool.execute(
      `SELECT sr.year, sr.category_id, sr.category_name, sr.position,
              sr.rider_id, sr.rider_full_name, sr.rider_number,
              sr.team_name, sr.constructor_name, sr.points,
              tr.team_color
       FROM standing_riders sr
       LEFT JOIN TeamRiders tr
         ON tr.year = sr.year
         AND tr.category_id = sr.category_id
         AND tr.rider_full_name = sr.rider_full_name
       WHERE sr.year = ? AND sr.category_id = ?
       ORDER BY sr.position ASC`,
      [year, categoryId]
    )

    return NextResponse.json(rows)
  } catch (error) {
    console.error('Error fetching standings:', error)
    return NextResponse.json({ error: 'Failed to fetch standings' }, { status: 500 })
  }
}
