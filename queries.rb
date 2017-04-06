class Queries
  def add_routes
    "INSERT INTO \"Routes\"
              SELECT DISTINCT \"LineID\", \"StopID\"
              FROM \"StartEnd\" as s
              NATURAL JOIN \"BusTimeData\""
  end
  def get_average_time (origin, destination,day, time_min, time_max,wet)
     "WITH journeys AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\"
        FROM \"StartEnd\"
        WHERE \"Day\" = #{day}
     ),
     lines AS (
        SELECT get_lines_with_stops(#{origin},#{destination}) as line_id
     ),
     starts AS (
        SELECT * FROM (
          SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as start_time
          FROM \"BusTimeData\"
          WHERE \"StopID\" = #{origin}
          AND get_weather(\"Timestamp\") = #{wet}
          GROUP BY \"LineID\", \"JourneyID\", \"Date\"
        ) as journeys
        WHERE CAST(journeys.start_time AS TIME) BETWEEN CAST('#{time_min}' AS TIME) AND CAST('#{time_max}' AS TIME)
     ),
     ends AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as end_time
        FROM \"BusTimeData\"
        WHERE \"StopID\" = #{destination}
        GROUP BY \"LineID\", \"JourneyID\", \"Date\"
     )
     SELECT * FROM (
        SELECT \"LineID\", AVG(j.t2 - j.t1) as avg FROM(
          SELECT \"LineID\", starts.start_time as t1, ends.end_time as t2
          FROM starts NATURAL JOIN ends NATURAL JOIN journeys JOIN lines ON (ends.\"LineID\" = lines.line_id)
        ) as j
     GROUP BY \"LineID\"
    ) as grouped
    ORDER BY grouped.avg ASC"
  end

  def get_average_time_ignore_weather(origin, destination, day, time_min, time_max)
    "WITH journeys AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\"
        FROM \"StartEnd\"
        WHERE \"Day\" = #{day}
     ),
     lines AS (
        SELECT get_lines_with_stops(#{origin},#{destination}) as line_id
     ),
     starts AS (
        SELECT * FROM (
          SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as start_time
          FROM \"BusTimeData\"
          WHERE \"StopID\" = #{origin}
          GROUP BY \"LineID\", \"JourneyID\", \"Date\"
        ) as journeys
        WHERE CAST(journeys.start_time AS TIME) BETWEEN CAST('#{time_min}' AS TIME) AND CAST('#{time_max}' AS TIME)
     ),
     ends AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as end_time
        FROM \"BusTimeData\"
        WHERE \"StopID\" = #{destination}
        GROUP BY \"LineID\", \"JourneyID\", \"Date\"
     )
    SELECT * FROM (
       SELECT \"LineID\", AVG(j.t2 - j.t1) as avg FROM(
          SELECT \"LineID\", starts.start_time as t1, ends.end_time as t2
          FROM starts NATURAL JOIN ends NATURAL JOIN journeys JOIN lines ON (ends.\"LineID\" = lines.line_id)
       ) as j
       GROUP BY \"LineID\"
    ) as grouped
    ORDER BY grouped.avg"
  end

  def get_prev(origin, destination, time,day)
    "with lines AS (
	      SELECT get_lines_with_stops(#{origin}, #{destination}) as line_id
    ),
    starts AS (
        SELECT * FROM (
          SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as start_time
          FROM \"BusTimeData\"
          WHERE \"StopID\" = #{origin}
          AND \"Day\" = #{day}
          GROUP BY \"LineID\", \"JourneyID\", \"Date\"
        ) as journeys
        WHERE CAST(journeys.start_time AS TIME) <  CAST('#{time}' AS TIME)
    ),
    ends AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as end_time
        FROM \"BusTimeData\"
        WHERE \"StopID\" = #{destination}
        GROUP BY \"LineID\", \"JourneyID\", \"Date\"
    )

    SELECT MAX(CAST(start_time AS TIME)) as time
    FROM starts NATURAL JOIN ends JOIN lines ON (\"LineID\" = lines.line_id)"
  end

  def get_next(origin, destination, time,day)
    "with lines AS (
	      SELECT get_lines_with_stops(#{origin}, #{destination}) as line_id
    ),
    starts AS (
        SELECT * FROM (
          SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as start_time
          FROM \"BusTimeData\"
          WHERE \"StopID\" = #{origin}
          AND \"Day\" = #{day}
          GROUP BY \"LineID\", \"JourneyID\", \"Date\"
        ) as journeys
        WHERE CAST(journeys.start_time AS TIME) > CAST('#{time}' AS TIME)
    ),
    ends AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as end_time
        FROM \"BusTimeData\"
        WHERE \"StopID\" = #{destination}
        GROUP BY \"LineID\", \"JourneyID\", \"Date\"
    )

    SELECT MIN(CAST(start_time AS TIME)) as time
    FROM starts NATURAL JOIN ends JOIN lines ON (\"LineID\" = lines.line_id)"
  end

  def get_average_time_for_hour(day, origin, destination, time_min, time_max)
    "WITH journeys AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\"
        FROM \"StartEnd\"
        WHERE \"Day\" = #{day}
     ),
     lines AS (
        SELECT get_lines_with_stops(#{origin},#{destination}) as line_id
     ),
     starts AS (
        SELECT * FROM (
          SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as start_time
          FROM \"BusTimeData\"
          WHERE \"StopID\" = #{origin}
          GROUP BY \"LineID\", \"JourneyID\", \"Date\"
        ) as journeys
        WHERE CAST(journeys.start_time AS TIME) BETWEEN CAST('#{time_min}' AS TIME) AND CAST('#{time_max}' AS TIME)
     ),
     ends AS (
        SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as end_time
        FROM \"BusTimeData\"
        WHERE \"StopID\" = #{destination}
        GROUP BY \"LineID\", \"JourneyID\", \"Date\"
     )
     SELECT AVG(ends.end_time- starts.start_time) as avg
     FROM starts NATURAL JOIN ends NATURAL JOIN journeys JOIN lines ON (ends.\"LineID\" = lines.line_id)
    "
  end

  def get_all_journeys(day,origin, destination, time_min, time_max)
    "WITH journeys AS (
      SELECT \"LineID\", \"JourneyID\", \"Date\"
      FROM \"StartEnd\"
      WHERE \"Day\" = #{day}
    ),
    lines AS (
      SELECT get_lines_with_stops(#{origin}, #{destination}) as line_id
    ),
    starts AS (
	    SELECT * FROM (
        SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as start_time
        FROM \"BusTimeData\"
    	WHERE \"StopID\" = #{origin}
    	GROUP BY (\"LineID\", \"JourneyID\", \"Date\")
    ) as journeys
    WHERE CAST(journeys.start_time AS TIME) BETWEEN '#{time_min}' AND '#{time_max}'
    ),
    ends AS (
      SELECT * FROM (
        SELECT \"LineID\", \"JourneyID\", \"Date\", MIN(\"Timestamp\") as end_time
        FROM \"BusTimeData\"
    	  WHERE \"StopID\" = #{destination}
    	  GROUP BY (\"LineID\", \"JourneyID\", \"Date\")
      ) as journeys
    )

    SELECT (ends.end_time - starts.start_time) as journey_time, get_weather(starts.start_time) as weather
    FROM starts NATURAL JOIN ends NATURAL JOIN journeys JOIN lines ON
       (ends.\"LineID\" = lines.line_id)"
  end
end