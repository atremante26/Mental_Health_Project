import { useEffect, useState } from "react"
import {
  LineChart, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid, ResponsiveContainer
} from "recharts"

function TimeSeriesChart() {
  const [data, setData] = useState([])

  useEffect(() => {
    fetch("https://mental-health-project-bct5.onrender.com/timeseries")
      .then(res => res.json())
      .then(setData)
      .catch(console.error)
  }, [])

  return (
    <div style={{ padding: "1rem" }}>
      <h2>Mental Health Time Series</h2>
      {data.length > 0 ? (
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="anxiety" stroke="#8884d8" />
            <Line type="monotone" dataKey="depression" stroke="#82ca9d" />
            <Line type="monotone" dataKey="sleep_issues" stroke="#ffc658" />
          </LineChart>
        </ResponsiveContainer>
      ) : (
        <p>Loading chart data...</p>
      )}
    </div>
  )
}

export default TimeSeriesChart
