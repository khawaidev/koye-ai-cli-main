

/**
 * KOYE Main Server - Multi-DB Chat System
 * 
 * Responsibilities:
 * - WebSocket endpoint for streaming chat
 * - REST endpoints for chat sessions and messages
 * - Asset generation (image, audio, 3D, video)
 * - LLM orchestration (Gemini 2.5 Flash)
 * - Multi-database management for chat storage
 */

import { createClient } from '@supabase/supabase-js';
import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import http from 'http';
import jwt from 'jsonwebtoken';
import morgan from 'morgan';
import multer from 'multer';
import fetch from 'node-fetch';
import { v4 as uuidv4 } from 'uuid';
import { WebSocketServer } from 'ws';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3002;

// Create HTTP server for both Express and WebSocket
const server = http.createServer(app);

// ============== MULTI-DATABASE MANAGER ==============

class MultiDbManager {
    constructor() {
        this.mainDb = null;
        this.chatDbs = new Map();
        this.dbConfigs = [];
        this.currentActiveDb = 'db1';
        this.maxRowsPerDb = parseInt(process.env.MAX_ROWS_PER_DB || '1000000');

        this.initializeDatabases();
    }

    initializeDatabases() {
        // Initialize main database
        this.mainDb = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_SERVICE_ROLE_KEY
        );

        // Load chat database configurations (db1, db2, db3, etc.)
        let dbIndex = 1;
        while (true) {
            const url = process.env[`SUPABASE_CHAT_DB${dbIndex}_URL`];
            const serviceKey = process.env[`SUPABASE_CHAT_DB${dbIndex}_SERVICE_KEY`];

            if (!url || !serviceKey) break;

            const dbId = `db${dbIndex}`;
            this.dbConfigs.push({ id: dbId, url, serviceKey });
            this.chatDbs.set(dbId, createClient(url, serviceKey));

            console.log(`  📦 Chat DB${dbIndex} loaded`);
            dbIndex++;
        }

        if (this.chatDbs.size === 0) {
            console.warn('⚠️  No chat databases configured! Using main DB for chats.');
            // Fallback: use main DB for chats
            this.chatDbs.set('db1', this.mainDb);
            this.dbConfigs.push({ id: 'db1', url: process.env.SUPABASE_URL, serviceKey: process.env.SUPABASE_SERVICE_ROLE_KEY });
        }

        console.log(`  📊 Total chat databases: ${this.chatDbs.size}`);
    }

    getMainDb() {
        return this.mainDb;
    }

    getChatDb(dbId) {
        return this.chatDbs.get(dbId) || null;
    }

    getActiveChatDb() {
        return this.chatDbs.get(this.currentActiveDb);
    }

    getActiveDbId() {
        return this.currentActiveDb;
    }

    getAllChatDbs() {
        return this.chatDbs;
    }

    async checkDbStatus(dbId) {
        const db = this.chatDbs.get(dbId);
        if (!db) return null;

        try {
            const { data, error } = await db
                .from('db_stats')
                .select('*')
                .single();

            if (error) return { id: dbId, isFull: false, usagePercent: 0 };

            return {
                id: dbId,
                isFull: data.is_full,
                usagePercent: ((data.total_sessions + data.total_messages) / data.max_rows) * 100,
                totalSessions: data.total_sessions,
                totalMessages: data.total_messages
            };
        } catch (e) {
            return { id: dbId, isFull: false, usagePercent: 0 };
        }
    }

    async ensureActiveDbAvailable() {
        const status = await this.checkDbStatus(this.currentActiveDb);

        if (status && status.isFull) {
            // Find next available database
            for (const config of this.dbConfigs) {
                if (config.id === this.currentActiveDb) continue;

                const nextStatus = await this.checkDbStatus(config.id);
                if (!nextStatus.isFull) {
                    this.currentActiveDb = config.id;
                    console.log(`Switched to chat database: ${config.id}`);

                    // Update in main DB
                    await this.mainDb
                        .from('system_settings')
                        .update({ value: JSON.stringify(config.id) })
                        .eq('key', 'active_chat_db');

                    return this.getActiveChatDb();
                }
            }
            throw new Error('All chat databases are full');
        }

        return this.getActiveChatDb();
    }

    async getUserDbId(userId) {
        // Check where user's data is stored
        const { data } = await this.mainDb
            .from('user_db_mapping')
            .select('db_id')
            .eq('user_id', userId)
            .order('last_used_at', { ascending: false })
            .limit(1)
            .single();

        return data?.db_id || this.currentActiveDb;
    }

    async recordUserDb(userId, dbId) {
        await this.mainDb
            .from('user_db_mapping')
            .upsert({
                user_id: userId,
                db_id: dbId,
                last_used_at: new Date().toISOString()
            }, {
                onConflict: 'user_id,db_id'
            });
    }
}

const dbManager = new MultiDbManager();

// Multer for file uploads
const upload = multer({ storage: multer.memoryStorage() });

// ============== MULTI-API FALLBACK MANAGER ==============

class MultiApiManager {
    constructor() {
        this.apis = {
            gemini: [],
            pixazo: [],
            rapidapi: [],
            hitem3d: [],
            kie: []
        };
        this.currentIndex = {
            gemini: 0,
            pixazo: 0,
            rapidapi: 0,
            hitem3d: 0,
            kie: 0
        };
        this.loadApiKeys();
    }

    loadApiKeys() {
        // Load Gemini API keys (GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc.)
        let idx = 1;
        while (process.env[`GEMINI_API_KEY_${idx}`]) {
            this.apis.gemini.push(process.env[`GEMINI_API_KEY_${idx}`]);
            idx++;
        }
        // Fallback to single key format
        if (this.apis.gemini.length === 0 && process.env.GEMINI_API_KEY) {
            this.apis.gemini.push(process.env.GEMINI_API_KEY);
        }

        // Load Pixazo API keys
        idx = 1;
        while (process.env[`PIXAZO_API_KEY_${idx}`]) {
            this.apis.pixazo.push(process.env[`PIXAZO_API_KEY_${idx}`]);
            idx++;
        }
        if (this.apis.pixazo.length === 0 && process.env.PIXAZO_API_KEY) {
            this.apis.pixazo.push(process.env.PIXAZO_API_KEY);
        }

        // Load RapidAPI keys
        idx = 1;
        while (process.env[`RAPIDAPI_KEY_${idx}`]) {
            this.apis.rapidapi.push(process.env[`RAPIDAPI_KEY_${idx}`]);
            idx++;
        }
        if (this.apis.rapidapi.length === 0 && process.env.RAPIDAPI_KEY) {
            this.apis.rapidapi.push(process.env.RAPIDAPI_KEY);
        }

        // Load Hitem3D credentials (paired)
        idx = 1;
        while (process.env[`HITEM3D_CLIENT_ID_${idx}`] && process.env[`HITEM3D_CLIENT_SECRET_${idx}`]) {
            this.apis.hitem3d.push({
                clientId: process.env[`HITEM3D_CLIENT_ID_${idx}`],
                clientSecret: process.env[`HITEM3D_CLIENT_SECRET_${idx}`]
            });
            idx++;
        }
        if (this.apis.hitem3d.length === 0 && process.env.HITEM3D_CLIENT_ID && process.env.HITEM3D_CLIENT_SECRET) {
            this.apis.hitem3d.push({
                clientId: process.env.HITEM3D_CLIENT_ID,
                clientSecret: process.env.HITEM3D_CLIENT_SECRET
            });
        }

        // Load KIE API keys
        idx = 1;
        while (process.env[`KIE_API_KEY_${idx}`]) {
            this.apis.kie.push(process.env[`KIE_API_KEY_${idx}`]);
            idx++;
        }
        if (this.apis.kie.length === 0 && process.env.KIE_API_KEY) {
            this.apis.kie.push(process.env.KIE_API_KEY);
        }

        console.log(`  🔑 API Keys loaded:`);
        console.log(`     - Gemini: ${this.apis.gemini.length} key(s)`);
        console.log(`     - Pixazo: ${this.apis.pixazo.length} key(s)`);
        console.log(`     - RapidAPI: ${this.apis.rapidapi.length} key(s)`);
        console.log(`     - Hitem3D: ${this.apis.hitem3d.length} credential(s)`);
        console.log(`     - KIE: ${this.apis.kie.length} key(s)`);
    }

    getKey(service) {
        const keys = this.apis[service];
        if (!keys || keys.length === 0) return null;
        return keys[this.currentIndex[service]];
    }

    rotateKey(service) {
        const keys = this.apis[service];
        if (!keys || keys.length <= 1) return false;

        this.currentIndex[service] = (this.currentIndex[service] + 1) % keys.length;
        console.log(`  ⚠️  Rotated ${service} API key to index ${this.currentIndex[service]}`);
        return true;
    }

    hasKey(service) {
        return this.apis[service] && this.apis[service].length > 0;
    }

    getKeyCount(service) {
        return this.apis[service]?.length || 0;
    }

    // Execute a function with automatic fallback to next API key on failure
    async withFallback(service, fn) {
        const keys = this.apis[service];
        if (!keys || keys.length === 0) {
            throw new Error(`No API keys configured for ${service}`);
        }

        const startIndex = this.currentIndex[service];
        let lastError = null;

        for (let attempts = 0; attempts < keys.length; attempts++) {
            try {
                const key = this.getKey(service);
                return await fn(key);
            } catch (error) {
                lastError = error;
                console.warn(`  ⚠️  ${service} API key ${this.currentIndex[service]} failed: ${error.message}`);

                // Rotate to next key
                if (!this.rotateKey(service)) {
                    break; // Only one key available
                }

                // If we've tried all keys, stop
                if (this.currentIndex[service] === startIndex) {
                    break;
                }
            }
        }

        throw lastError || new Error(`All ${service} API keys failed`);
    }
}

const apiManager = new MultiApiManager();

// Credit costs (from credit-cost-cli.md)
const CREDIT_COSTS = {
    chat: 50, // per 1M tokens
    image_gen: 10,
    audio_gen_per_sec: 5,
    video_gen_per_sec: 10,
    model_3d_512: 25,
    model_3d_1024: 50,
    model_3d_1536: 90
};

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(morgan('dev'));

// ============== AUTH MIDDLEWARE ==============

const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) {
        return res.status(401).json({ success: false, error: 'Authentication required' });
    }

    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) {
            return res.status(403).json({ success: false, error: 'Invalid or expired token' });
        }
        req.user = user;
        next();
    });
};

// ============== CREDIT MANAGEMENT ==============

async function deductCredits(userId, amount, actionType, details = {}, referenceId = null) {
    const mainDb = dbManager.getMainDb();

    const { data, error } = await mainDb.rpc('deduct_credits', {
        p_user_id: userId,
        p_amount: amount,
        p_action_type: actionType,
        p_action_details: details,
        p_reference_id: referenceId
    });

    if (error) throw error;
    if (!data[0]?.success) {
        throw new Error(data[0]?.error_message || 'Failed to deduct credits');
    }

    return data[0].new_balance;
}

async function getUserCredits(userId) {
    const mainDb = dbManager.getMainDb();

    const { data, error } = await mainDb
        .from('user_profiles')
        .select('credits_remaining, plan')
        .eq('id', userId)
        .single();

    if (error) throw error;
    return data;
}

// ============== SYSTEM PROMPT ==============

const SYSTEM_PROMPT = `You are KOYE AI, an expert in game design, game coding, and asset creation for both 2D and 3D games.
You help users design game assets through deep conversations and accuracy.

CREDIT COSTS:
- Chat: 50 credits per 1M tokens
- Image generation: 10 credits per image
- Audio generation: 5 credits per second
- Video generation: 10 credits per second  
- 3D Model (512p): 25 credits
- 3D Model (1024p): 50 credits
- 3D Model (1536p): 90 credits

When you need to generate assets, output them in this JSON format:
\`\`\`koye-action
{
  "action": "generate_image" | "generate_3d" | "generate_audio" | "generate_video" | "create_file",
  "params": { ... }
}
\`\`\`

Always inform users of credit costs before generating.`;

// ============== GEMINI CHAT ==============

async function* streamGeminiChat(messages, systemPrompt = SYSTEM_PROMPT) {
    if (!apiManager.hasKey('gemini')) throw new Error('No Gemini API keys configured');

    const contents = [
        { role: 'user', parts: [{ text: systemPrompt }] },
        { role: 'model', parts: [{ text: 'Hello! I\'m KOYE AI. What would you like to create?' }] },
        ...messages.map(m => ({
            role: m.role === 'assistant' ? 'model' : 'user',
            parts: [{ text: m.content }]
        }))
    ];

    // For streaming, we use the current key (fallback on error would break the stream)
    const apiKey = apiManager.getKey('gemini');

    const response = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:streamGenerateContent?key=${apiKey}&alt=sse`,
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ contents, generationConfig: { maxOutputTokens: 8192, temperature: 0.7 } })
        }
    );

    if (!response.ok) {
        apiManager.rotateKey('gemini'); // Rotate for next request
        throw new Error(`Gemini API error: ${response.status}`);
    }

    const reader = response.body;
    let buffer = '';

    for await (const chunk of reader) {
        buffer += chunk.toString();
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
            if (line.startsWith('data: ')) {
                try {
                    const data = JSON.parse(line.slice(6));
                    if (data.candidates?.[0]?.content?.parts?.[0]?.text) {
                        yield data.candidates[0].content.parts[0].text;
                    }
                } catch (e) { }
            }
        }
    }
}

async function sendGeminiChat(messages, systemPrompt = SYSTEM_PROMPT) {
    return await apiManager.withFallback('gemini', async (apiKey) => {
        const contents = [
            { role: 'user', parts: [{ text: systemPrompt }] },
            { role: 'model', parts: [{ text: 'Hello! I\'m KOYE AI.' }] },
            ...messages.map(m => ({
                role: m.role === 'assistant' ? 'model' : 'user',
                parts: [{ text: m.content }]
            }))
        ];

        const response = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${apiKey}`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ contents, generationConfig: { maxOutputTokens: 8192, temperature: 0.7 } })
            }
        );

        if (!response.ok) throw new Error(`Gemini API error: ${response.status}`);

        const data = await response.json();
        return data.candidates?.[0]?.content?.parts?.[0]?.text || '';
    });
}

// ============== ASSET GENERATION (with Multi-API Fallback) ==============

async function generateImageWithPixazo(prompt, options = {}) {
    return await apiManager.withFallback('pixazo', async (apiKey) => {
        const response = await fetch('https://gateway.pixazo.ai/byteplus/v1/getTextToImage', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Ocp-Apim-Subscription-Key': apiKey },
            body: JSON.stringify({
                model: options.model || 'seedream-3-0-t2i-250415',
                prompt: prompt.trim(),
                size: options.size || '1024x1024',
                guidance_scale: 2.5,
                watermark: false
            })
        });

        if (!response.ok) throw new Error(`Pixazo error: ${response.status}`);
        const data = await response.json();
        return data.data?.[0]?.url;
    });
}

async function generateVideoWithVeo(prompt, options = {}) {
    return await apiManager.withFallback('pixazo', async (apiKey) => {
        const response = await fetch('https://gateway.pixazo.ai/veo/v1/veo-3.1/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Ocp-Apim-Subscription-Key': apiKey },
            body: JSON.stringify({
                prompt: prompt.trim(),
                aspect_ratio: options.aspect_ratio || '16:9',
                duration: options.duration || 8,
                resolution: options.resolution || '1080p',
                generate_audio: true
            })
        });

        if (!response.ok) throw new Error(`Veo error: ${response.status}`);
        return await response.json();
    });
}

async function generateAudioWithElevenLabs(text, options = {}) {
    return await apiManager.withFallback('rapidapi', async (apiKey) => {
        const response = await fetch('https://elevenlabs-sound-effects.p.rapidapi.com/generate-sound', {
            method: 'POST',
            headers: {
                'x-rapidapi-key': apiKey,
                'x-rapidapi-host': 'elevenlabs-sound-effects.p.rapidapi.com',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ text: text.trim(), prompt_influence: 0.3, duration_seconds: options.duration_seconds || null })
        });

        if (!response.ok) throw new Error(`ElevenLabs error: ${response.status}`);
        const data = await response.json();
        return { base64: data.data?.[0]?.content_base64, contentType: data.data?.[0]?.content_type || 'audio/mpeg' };
    });
}

// ============== CHAT SESSIONS (Multi-DB) ==============

app.post('/chat/sessions', authenticateToken, async (req, res) => {
    try {
        const { project_id, title } = req.body;
        const userId = req.user.user_id;

        // Get active chat database
        const chatDb = await dbManager.ensureActiveDbAvailable();
        const dbId = dbManager.getActiveDbId();

        const sessionId = uuidv4();

        const { data, error } = await chatDb
            .from('chat_sessions')
            .insert({
                id: sessionId,
                user_id: userId,
                project_id: project_id || null,
                title: title || 'Untitled Session',
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            })
            .select()
            .single();

        if (error) throw error;

        // Record user-db mapping
        await dbManager.recordUserDb(userId, dbId);

        res.json({ success: true, session: { ...data, db_id: dbId } });
    } catch (error) {
        console.error('Create session error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/chat/sessions', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.user_id;
        const allSessions = [];

        // Query all chat databases for user's sessions
        for (const [dbId, chatDb] of dbManager.getAllChatDbs()) {
            try {
                const { data, error } = await chatDb
                    .from('chat_sessions')
                    .select('*')
                    .eq('user_id', userId)
                    .order('updated_at', { ascending: false });

                if (!error && data) {
                    allSessions.push(...data.map(s => ({ ...s, db_id: dbId })));
                }
            } catch (e) {
                console.warn(`Error querying ${dbId}:`, e.message);
            }
        }

        // Sort by updated_at
        allSessions.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));

        res.json({ success: true, sessions: allSessions });
    } catch (error) {
        console.error('Fetch sessions error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

app.delete('/chat/sessions/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { db_id } = req.query;

        if (!db_id) {
            return res.status(400).json({ success: false, error: 'db_id query param required' });
        }

        const chatDb = dbManager.getChatDb(db_id);
        if (!chatDb) {
            return res.status(400).json({ success: false, error: 'Invalid db_id' });
        }

        await chatDb.from('chat_messages').delete().eq('session_id', id);
        await chatDb.from('chat_sessions').delete().eq('id', id).eq('user_id', req.user.user_id);

        res.json({ success: true });
    } catch (error) {
        console.error('Delete session error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============== CHAT MESSAGES (Multi-DB) ==============

app.get('/chat/sessions/:id/messages', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { db_id } = req.query;

        if (!db_id) {
            return res.status(400).json({ success: false, error: 'db_id query param required' });
        }

        const chatDb = dbManager.getChatDb(db_id);
        if (!chatDb) {
            return res.status(400).json({ success: false, error: 'Invalid db_id' });
        }

        const { data, error } = await chatDb
            .from('chat_messages')
            .select('*')
            .eq('session_id', id)
            .order('created_at', { ascending: true });

        if (error) throw error;

        res.json({ success: true, messages: data });
    } catch (error) {
        console.error('Fetch messages error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/chat/sessions/:id/messages', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { content, db_id } = req.body;
        const userId = req.user.user_id;

        // Determine which database to use
        let chatDbId = db_id;
        if (!chatDbId) {
            chatDbId = await dbManager.getUserDbId(userId);
        }

        const chatDb = dbManager.getChatDb(chatDbId);
        if (!chatDb) {
            return res.status(400).json({ success: false, error: 'Chat database not found' });
        }

        // Check user credits first
        const credits = await getUserCredits(userId);
        if (credits.credits_remaining < 1) {
            return res.status(402).json({ success: false, error: 'Insufficient credits' });
        }

        // Save user message
        await chatDb.from('chat_messages').insert({
            id: uuidv4(),
            session_id: id,
            user_id: userId,
            role: 'user',
            content,
            created_at: new Date().toISOString()
        });

        // Get history
        const { data: history } = await chatDb
            .from('chat_messages')
            .select('role, content')
            .eq('session_id', id)
            .order('created_at', { ascending: true })
            .limit(20);

        // Generate AI response
        const reply = await sendGeminiChat(history || []);

        // Parse and execute actions
        const actions = parseActions(reply);
        const actionResults = await executeActions(actions, userId);

        // Calculate tokens (rough estimate)
        const tokensUsed = Math.ceil((content.length + reply.length) / 4);
        const chatCredits = Math.ceil((tokensUsed / 1000000) * CREDIT_COSTS.chat);

        // Deduct credits for chat
        if (chatCredits > 0) {
            await deductCredits(userId, chatCredits, 'chat', { tokens: tokensUsed, session_id: id }, id);
        }

        // Save assistant message
        await chatDb.from('chat_messages').insert({
            id: uuidv4(),
            session_id: id,
            user_id: userId,
            role: 'assistant',
            content: reply.replace(/```koye-action[\s\S]*?```/g, '').trim(),
            metadata: { tokens: tokensUsed, model: 'gemini-2.5-flash' },
            actions: actionResults,
            created_at: new Date().toISOString()
        });

        // Update session
        await chatDb.from('chat_sessions').update({ updated_at: new Date().toISOString() }).eq('id', id);

        res.json({
            success: true,
            reply: reply.replace(/```koye-action[\s\S]*?```/g, '').trim(),
            actions: actionResults,
            credits_used: chatCredits,
            db_id: chatDbId
        });
    } catch (error) {
        console.error('Send message error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

function parseActions(text) {
    const actions = [];
    const regex = /```koye-action\s*([\s\S]*?)```/g;
    let match;
    while ((match = regex.exec(text)) !== null) {
        try { actions.push(JSON.parse(match[1].trim())); }
        catch (e) { }
    }
    return actions;
}

async function executeActions(actions, userId) {
    const results = [];

    for (const action of actions) {
        try {
            let result;
            let creditCost = 0;

            switch (action.action) {
                case 'generate_image':
                    creditCost = CREDIT_COSTS.image_gen;
                    await deductCredits(userId, creditCost, 'image_gen', { prompt: action.params.prompt });
                    result = await generateImageWithPixazo(action.params.prompt, action.params);
                    results.push({ action: 'generate_image', success: true, url: result, credits: creditCost });
                    break;

                case 'generate_video':
                    creditCost = CREDIT_COSTS.video_gen_per_sec * (action.params.duration || 8);
                    await deductCredits(userId, creditCost, 'video_gen', action.params);
                    result = await generateVideoWithVeo(action.params.prompt, action.params);
                    results.push({ action: 'generate_video', success: true, ...result, credits: creditCost });
                    break;

                case 'generate_audio':
                    creditCost = CREDIT_COSTS.audio_gen_per_sec * (action.params.duration_seconds || 5);
                    await deductCredits(userId, creditCost, 'audio_gen', action.params);
                    result = await generateAudioWithElevenLabs(action.params.prompt, action.params);
                    results.push({ action: 'generate_audio', success: true, ...result, credits: creditCost });
                    break;

                case 'create_file':
                    results.push({ action: 'create_file', success: true, path: action.params.path, content: action.params.content });
                    break;

                default:
                    results.push({ action: action.action, success: false, error: 'Unknown action' });
            }
        } catch (error) {
            results.push({ action: action.action, success: false, error: error.message });
        }
    }

    return results;
}

// ============== DATABASE STATUS ==============

app.get('/db/status', authenticateToken, async (req, res) => {
    try {
        const statuses = [];

        for (const [dbId, _] of dbManager.getAllChatDbs()) {
            const status = await dbManager.checkDbStatus(dbId);
            statuses.push(status);
        }

        res.json({
            success: true,
            current_active: dbManager.getActiveDbId(),
            databases: statuses
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============== ASSET GENERATION ENDPOINTS ==============

app.post('/generate/image', authenticateToken, async (req, res) => {
    try {
        const { prompt, model, size } = req.body;
        const creditCost = CREDIT_COSTS.image_gen;

        await deductCredits(req.user.user_id, creditCost, 'image_gen', { prompt });
        const url = await generateImageWithPixazo(prompt, { model, size });

        res.json({ success: true, url, credits_used: creditCost });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/generate/video', authenticateToken, async (req, res) => {
    try {
        const duration = req.body.duration || 8;
        const creditCost = CREDIT_COSTS.video_gen_per_sec * duration;

        await deductCredits(req.user.user_id, creditCost, 'video_gen', req.body);
        const result = await generateVideoWithVeo(req.body.prompt, req.body);

        res.json({ success: true, ...result, credits_used: creditCost });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/generate/audio', authenticateToken, async (req, res) => {
    try {
        const duration = req.body.duration_seconds || 5;
        const creditCost = CREDIT_COSTS.audio_gen_per_sec * duration;

        await deductCredits(req.user.user_id, creditCost, 'audio_gen', req.body);
        const result = await generateAudioWithElevenLabs(req.body.prompt, req.body);

        res.json({ success: true, ...result, credits_used: creditCost });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============== WEBSOCKET FOR STREAMING ==============

const wss = new WebSocketServer({ server, path: '/ws/chat' });

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const token = url.searchParams.get('token');

    if (!token) {
        ws.send(JSON.stringify({ type: 'error', error: 'Auth required' }));
        ws.close();
        return;
    }

    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) {
            ws.send(JSON.stringify({ type: 'error', error: 'Invalid token' }));
            ws.close();
            return;
        }
        ws.user = user;
        ws.send(JSON.stringify({ type: 'connected' }));
    });

    ws.on('message', async (data) => {
        try {
            const msg = JSON.parse(data);
            if (msg.type === 'chat') {
                await handleStreamingChat(ws, msg);
            }
        } catch (error) {
            ws.send(JSON.stringify({ type: 'error', error: error.message }));
        }
    });
});

async function handleStreamingChat(ws, msg) {
    const { session_id, content, db_id } = msg;
    const userId = ws.user.user_id;

    try {
        const chatDbId = db_id || await dbManager.getUserDbId(userId);
        const chatDb = dbManager.getChatDb(chatDbId);

        await chatDb.from('chat_messages').insert({
            id: uuidv4(), session_id, user_id: userId, role: 'user', content, created_at: new Date().toISOString()
        });

        const { data: history } = await chatDb
            .from('chat_messages')
            .select('role, content')
            .eq('session_id', session_id)
            .order('created_at', { ascending: true })
            .limit(20);

        ws.send(JSON.stringify({ type: 'stream_start' }));

        let fullResponse = '';
        for await (const chunk of streamGeminiChat(history || [])) {
            fullResponse += chunk;
            ws.send(JSON.stringify({ type: 'stream_chunk', content: chunk }));
        }

        const actions = parseActions(fullResponse);
        const actionResults = await executeActions(actions, userId);

        await chatDb.from('chat_messages').insert({
            id: uuidv4(),
            session_id,
            user_id: userId,
            role: 'assistant',
            content: fullResponse.replace(/```koye-action[\s\S]*?```/g, '').trim(),
            actions: actionResults,
            created_at: new Date().toISOString()
        });

        ws.send(JSON.stringify({ type: 'stream_end', actions: actionResults, db_id: chatDbId }));
    } catch (error) {
        ws.send(JSON.stringify({ type: 'error', error: error.message }));
    }
}

// ============== HEALTH CHECK ==============

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        service: 'koye-main-server',
        version: '1.0.0',
        chat_databases: dbManager.getAllChatDbs().size,
        active_chat_db: dbManager.getActiveDbId(),
        apis: {
            gemini: { available: apiManager.hasKey('gemini'), keys: apiManager.getKeyCount('gemini') },
            pixazo: { available: apiManager.hasKey('pixazo'), keys: apiManager.getKeyCount('pixazo') },
            hitem3d: { available: apiManager.hasKey('hitem3d'), keys: apiManager.getKeyCount('hitem3d') },
            rapidapi: { available: apiManager.hasKey('rapidapi'), keys: apiManager.getKeyCount('rapidapi') },
            kie: { available: apiManager.hasKey('kie'), keys: apiManager.getKeyCount('kie') }
        }
    });
});

// Start server
server.listen(PORT, () => {
    console.log(`🚀 KOYE Main Server running on port ${PORT}`);
    console.log(`   Chat DBs: ${dbManager.getAllChatDbs().size}, Active: ${dbManager.getActiveDbId()}`);
});
