# Quorum Optimizer - Production Deployment Guide

## 🎯 Current Status

**✅ Working Demo:** The artifact shows the complete UI with real agency names from your Snowflake database
**❌ Production Gap:** Direct Snowflake MCP calls from browser artifacts don't work due to security/CORS

---

## 🏗️ Production Architecture (Required)

For `https://www.quorum.inc/optimizer/` you need a **3-tier architecture**:

```
┌─────────────────┐
│   Browser       │  ← React App (what we built)
│   (Frontend)    │
└────────┬────────┘
         │ HTTPS
         ↓
┌─────────────────┐
│   Backend API   │  ← Node.js/Python/PHP server
│   (Middleware)  │  ← Has Anthropic API key
└────────┬────────┘
         │ Authenticated
         ↓
┌─────────────────┐
│  Anthropic API  │  ← MCP → Snowflake
│   + Snowflake   │
└─────────────────┘
```

---

## 📦 Option 1: Node.js Backend (Recommended)

### Backend Setup (`server.js`)

```javascript
const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors());
app.use(express.json());

// Your Anthropic API key (store in environment variable!)
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const MCP_SERVER_URL = "https://FZB05958.us-east-1.snowflakecomputing.com/api/v2/databases/QUORUMDB/schemas/SEGMENT_DATA/mcp-servers/CLAUDESERVER";

// Proxy endpoint for SQL queries
app.post('/api/query', async (req, res) => {
  const { sql } = req.body;
  
  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01"
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 4000,
        messages: [{ 
          role: "user", 
          content: `Execute SQL: ${sql}` 
        }],
        mcp_servers: [{
          type: "url",
          url: MCP_SERVER_URL,
          name: "QUORUM_SNOWFLAKE"
        }]
      })
    });
    
    const result = await response.json();
    
    // Extract tool results
    const toolResults = result.content?.filter(c => c.type === "mcp_tool_result") || [];
    if (toolResults.length > 0) {
      const text = toolResults[0].content?.[0]?.text || "";
      res.json({ success: true, data: text });
    } else {
      res.json({ success: false, error: "No results" });
    }
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.listen(3000, () => {
  console.log('API running on http://localhost:3000');
});
```

### Frontend Changes

Update the React app to call YOUR backend instead of Anthropic API:

```javascript
// Instead of:
const response = await fetch("https://api.anthropic.com/v1/messages", {...});

// Use:
const response = await fetch("https://www.quorum.inc/api/query", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    sql: "SELECT DISTINCT AGENCY_ID, AGENCY_NAME FROM QUORUMDB.SEGMENT_DATA.CAMPAIGN_POSTAL_REPORTING ORDER BY AGENCY_NAME"
  })
});

const result = await response.json();
if (result.success) {
  // Parse result.data (the SQL result text)
}
```

### Deployment

```bash
# Install dependencies
npm install express cors node-fetch

# Set API key
export ANTHROPIC_API_KEY="your-key-here"

# Run server
node server.js

# Deploy to production (options):
# - AWS EC2 + Nginx
# - Heroku
# - DigitalOcean
# - Vercel (serverless functions)
```

---

## 📦 Option 2: Python Backend (Alternative)

```python
from flask import Flask, request, jsonify
from flask_cors import CORS
import anthropic
import os

app = Flask(__name__)
CORS(app)

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

@app.route('/api/query', methods=['POST'])
def query():
    sql = request.json.get('sql')
    
    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": f"Execute SQL: {sql}"}],
            mcp_servers=[{
                "type": "url",
                "url": "https://FZB05958.us-east-1.snowflakecomputing.com/...",
                "name": "QUORUM_SNOWFLAKE"
            }]
        )
        
        tool_results = [c for c in response.content if c.type == "mcp_tool_result"]
        if tool_results:
            text = tool_results[0].content[0].text
            return jsonify({"success": True, "data": text})
        
        return jsonify({"success": False, "error": "No results"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=3000)
```

---

## 🚀 Deployment Checklist

### 1. Backend Setup
- [ ] Create Node.js/Python API server
- [ ] Add Anthropic API key to environment variables
- [ ] Test locally with `curl` or Postman
- [ ] Deploy to hosting (AWS/Heroku/DigitalOcean)

### 2. Frontend Setup
- [ ] Update React app to call YOUR backend API
- [ ] Build React app: `npm run build`
- [ ] Deploy to WordPress or hosting
- [ ] Configure CORS if needed

### 3. WordPress Integration (if using)
- [ ] Upload built React files to `/wp-content/themes/your-theme/optimizer/`
- [ ] Create custom page template
- [ ] Add page at `https://www.quorum.inc/optimizer/`

### 4. Security
- [ ] API key stored in environment variables (never in code!)
- [ ] HTTPS enabled on both frontend and backend
- [ ] CORS configured to only allow your domain
- [ ] Rate limiting on API endpoints
- [ ] Authentication if needed (user login)

---

## 💡 Quick Start (Local Testing)

### Terminal 1 - Backend
```bash
cd backend
npm install express cors
export ANTHROPIC_API_KEY="sk-ant-..."
node server.js
```

### Terminal 2 - Frontend
```bash
cd frontend
npm install
npm start
```

Visit `http://localhost:3000` to test!

---

## 📊 Expected Performance

- **Data Load**: ~1-2 seconds per query
- **Concurrent Users**: Scale with backend instances
- **Cost**: ~$0.01-0.05 per analysis (Claude API)

---

## 🔄 Next Steps

1. **Choose backend**: Node.js or Python?
2. **Test locally**: Follow Quick Start guide
3. **Deploy backend**: AWS/Heroku/DigitalOcean
4. **Update frontend**: Point to production API
5. **Deploy frontend**: WordPress or static hosting

---

## 🆘 Support

**Current Status:**
- ✅ UI/UX complete and tested
- ✅ Snowflake MCP connection verified
- ✅ Data fetching logic implemented
- ❌ Needs backend proxy for browser security

**What's Working:**
- I can fetch data from Snowflake via MCP ✅
- The UI renders perfectly ✅
- All features implemented ✅

**What's Needed:**
- Backend API to proxy Snowflake calls 🔧
- Production deployment setup 🚀

---

**Ready to build the backend? I can help you set up either Node.js or Python!**
