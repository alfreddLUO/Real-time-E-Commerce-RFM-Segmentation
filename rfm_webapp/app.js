const express = require('express');
const { exec } = require('child_process');
const app = express();

const PORT = 3034;


const MASTER_IP = '172.31.81.29';      // Spark/Hive Master Node IP
const SSH_USER = 'hadoop';             // User on Master Node
const SSH_KEY = '/home/ec2-user/peiyuanluo/cluster_key.pem';
// ----------------------------------------------------------------------

function runHiveQueryRemote(sql) {
    return new Promise((resolve, reject) => {
        // 构造 SSH 命令
        const cmd = `ssh -o StrictHostKeyChecking=no -i ${SSH_KEY} ${SSH_USER}@${MASTER_IP} "hive -S -e \\"${sql}\\""`;

        console.log(`[RemoteShell] Executing on Master (${MASTER_IP})...`);

        exec(cmd, { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
            if (error) {
                console.error(`[RemoteShell] Failed: ${error.message}`);
                reject(error);
                return;
            }
            resolve(stdout.trim());
        });
    });
}

function getSegmentDescription(id) {
    const mapping = {
        0: 'Churn Risk',
        1: 'Loyal / Frequent',
        2: 'New / Promising',
        3: 'High Value VIP',
        "-1": 'Unclassified'
    };
    return mapping[String(id)] || 'Unknown';
}

app.use(express.static('public'));
app.use(express.json());

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/public/index.html');
});

// --- API: Batch Layer Summary (REAL HIVE DATA via SSH) ---
app.get('/api/batch_summary', async (req, res) => {
    try {
        console.log("Fetching REAL Batch Data from Hive (via SSH)...");

        const sql = `
            SELECT
                t2.SegmentID,
                COUNT(t1.CustomerID),
                AVG(t1.Recency),
                AVG(t1.Frequency),
                AVG(t1.Monetary)
            FROM default.peiyuanluo_ecom_user_rfm_batch t1
                     JOIN default.peiyuanluo_ecom_user_segments t2
                          ON t1.CustomerID = t2.CustomerID
            GROUP BY t2.SegmentID
            ORDER BY t2.SegmentID
        `;

        const rawOutput = await runHiveQueryRemote(sql);

        const rows = rawOutput.split('\n').filter(line => line.trim() !== '');

        const summary = rows.map(line => {
            const parts = line.split('\t');
            if(parts.length < 5) return null;
            return {
                SegmentID: parts[0],
                TotalCustomers: parseInt(parts[1]),
                AvgRecency: parseFloat(parts[2]).toFixed(2),
                AvgFrequency: parseFloat(parts[3]).toFixed(2),
                AvgMonetary: parseFloat(parts[4]).toFixed(2),
                Description: getSegmentDescription(parts[0])
            };
        }).filter(x => x !== null);

        console.log(`[Batch] Success: Fetched ${summary.length} rows.`);
        res.json({ status: 'ok', title: 'RFM Batch Summary', data: summary });

    } catch (error) {
        console.error('Batch API Error:', error);
        res.status(500).json({ status: 'error', message: 'Failed to fetch Hive data via SSH' });
    }
});

// --- API: Real-time Serving Lookup (REAL HIVE DATA via SSH) ---
app.get('/api/realtime_segment', async (req, res) => {
    const customerID = req.query.customer_id || 12345;
    const newOrderValue = parseFloat(req.query.order_value) || 150.00;

    try {
        console.log(`[Serving] Lookup Customer ${customerID} via SSH...`);

        const sql = `
            SELECT SegmentID
            FROM default.peiyuanluo_ecom_user_segments
            WHERE CustomerID = ${customerID}
        `;

        const rawOutput = await runHiveQueryRemote(sql);

        let segment = -1;
        if (rawOutput && rawOutput.trim().length > 0 && !isNaN(parseInt(rawOutput))) {
            segment = parseInt(rawOutput.trim());
        }

        console.log(`[Serving] Result: Segment ${segment}`);

        res.json({
            status: 'ok',
            title: 'Real-time Segment Lookup',
            data: {
                CustomerID: customerID,
                NewOrderValue: newOrderValue.toFixed(2),
                SegmentID: segment,
                SegmentDescription: getSegmentDescription(segment),
                Explanation: 'Fetched directly from Hive (via SSH Tunnel).'
            }
        });

    } catch (error) {
        console.error('Realtime API Error:', error);
        res.status(500).json({ status: 'error', message: 'Failed to lookup Hive via SSH' });
    }
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`Web App running on http://localhost:${PORT}`);
    console.log(`Target Master: ${MASTER_IP}`);
    console.log(`SSH Key: ${SSH_KEY}`);
    console.log("✅ Running in REAL DATA MODE (SSH)");
});