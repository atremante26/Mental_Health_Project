import { useEffect, useState } from 'react'

function App() {
  const [data, setData] = useState(null)

  useEffect(() => {
    fetch("https://mental-health-project-bct5.onrender.com/insights")
      .then(res => res.json())
      .then(setData)
      .catch(console.error)
  }, [])

  return (
    <div style={{ padding: "1rem", fontFamily: "sans-serif" }}>
      <h1>Mental Health Insights</h1>
      {data ? (
        <div>
          <p><strong>Summary:</strong> {data.summary}</p>
          <p><strong>Date Range:</strong> {data.date_range}</p>
        </div>
      ) : (
        <p>Loading insights...</p>
      )}
    </div>
  )
}

export default App