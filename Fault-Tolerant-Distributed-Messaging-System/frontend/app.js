const API_BASE = 'http://localhost:8000';

// State
let currentUser = null;
let selectedUser = null;
let usersList = [];
let messagesList = [];
let localPendingMessages = {}; // To store messages sent but not yet in DB
let pollingIntervals = {};

// DOM Elements
const authModal = document.getElementById('auth-modal');
const authForm = document.getElementById('auth-form');
const authError = document.getElementById('auth-error');
const appContainer = document.getElementById('app-container');
const currentUserBadge = document.getElementById('current-user-badge');
const sidebarUsers = document.getElementById('users-list');

const emptyState = document.getElementById('empty-state');
const activeChat = document.getElementById('active-chat');
const chatWithUsername = document.getElementById('chat-with-username');
const messagesContainer = document.getElementById('messages-container');
const messageForm = document.getElementById('message-form');
const messageInput = document.getElementById('message-input');

// Initialize
authForm.addEventListener('submit', handleLogin);
messageForm.addEventListener('submit', sendMessage);

// ---- Authentication & User Setup ----
async function handleLogin(e) {
    e.preventDefault();
    const userId = document.getElementById('userId').value.trim();
    const username = document.getElementById('username').value.trim();
    const email = document.getElementById('email').value.trim();

    try {
        const res = await fetch(`${API_BASE}/users`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ userId, username, email })
        });

        if (res.status === 409) {
            showError('User ID or username already exists! Choose another.');
            return;
        }

        if (!res.ok) throw new Error('Failed to connect to the network.');

        // Success
        currentUser = { userId, username, email };
        authModal.classList.add('hidden');
        appContainer.classList.remove('hidden');
        currentUserBadge.textContent = `${username} (${userId})`;

        startPolling();
    } catch (err) {
        showError('Could not connect to API. Make sure the backend is running.');
    }
}

function showError(msg) {
    authError.textContent = msg;
    authError.classList.remove('hidden');
}

// ---- Polling Logic ----
function startPolling() {
    pollUsers();
    pollingIntervals.users = setInterval(pollUsers, 3000); // every 3s
}

async function pollUsers() {
    try {
        const res = await fetch(`${API_BASE}/users`);
        if (!res.ok) return;
        const data = await res.json();
        usersList = data.users.filter(u => u.userId !== currentUser.userId);
        renderSidebar();
    } catch (err) {
        console.error('Error fetching users:', err);
    }
}

// ---- UI Rendering ----
function renderSidebar() {
    sidebarUsers.innerHTML = '';
    if (usersList.length === 0) {
        sidebarUsers.innerHTML = '<p style="color: var(--text-muted); font-size: 0.85rem; padding: 1rem; text-align: center;">No other users found. Open another tab and register!</p>';
        return;
    }

    usersList.forEach(user => {
        const div = document.createElement('div');
        div.className = `user-item ${selectedUser === user.userId ? 'active' : ''}`;
        div.innerHTML = `
            <div class="user-item-name">${user.username}</div>
            <div class="user-item-id">ID: ${user.userId}</div>
        `;
        div.onclick = () => selectUser(user);
        sidebarUsers.appendChild(div);
    });
}

function selectUser(user) {
    selectedUser = user.userId;
    chatWithUsername.textContent = user.username;
    
    emptyState.classList.add('hidden');
    activeChat.classList.remove('hidden');
    
    renderSidebar(); // update active state

    // clear prev chat interval
    if (pollingIntervals.chat) clearInterval(pollingIntervals.chat);
    
    // reset view immediately
    messagesList = [];
    localPendingMessages = {};
    renderMessages();

    // Start polling this specific chat heavily
    pollChat();
    pollingIntervals.chat = setInterval(pollChat, 1500); // 1.5s real-time feel
}

// ---- Messaging Logic ----
async function pollChat() {
    if (!selectedUser) return;
    try {
        const res = await fetch(`${API_BASE}/messages/${currentUser.userId}/${selectedUser}?limit=100`);
        if (!res.ok) return;
        const data = await res.json();
        messagesList = data.messages;
        
        // Remove from localPendingMessages if it appeared in the DB fetch
        messagesList.forEach(dbMsg => {
            if (localPendingMessages[dbMsg.messageId]) {
                delete localPendingMessages[dbMsg.messageId];
            }
        });

        renderMessages();
    } catch (err) {
        console.error('Error polling chat:', err);
    }
}

async function sendMessage(e) {
    e.preventDefault();
    if (!selectedUser) return;
    
    const content = messageInput.value.trim();
    if (!content) return;
    
    // Generate simple unique ID
    const msgId = 'msg_' + Date.now() + '_' + Math.floor(Math.random()*1000);
    const timestamp = Date.now();

    // 1. Optimistic UI Update (1 Tick)
    const localMsg = {
        messageId: msgId,
        fromUser: currentUser.userId,
        toUser: selectedUser,
        content: content,
        timestamp: timestamp,
        deliveryStatus: 'sent' // local tag for 1 tick
    };
    
    localPendingMessages[msgId] = localMsg;
    messageInput.value = '';
    renderMessages();
    
    // scroll to bottom
    messagesContainer.scrollTop = messagesContainer.scrollHeight;

    // 2. Send to Backend
    try {
        await fetch(`${API_BASE}/messages`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                messageId: msgId,
                fromUser: currentUser.userId,
                toUser: selectedUser,
                content: content
            })
        });
        // Note: we don't manipulate the tick here. We wait for the Poller to pull it from the DB.
        // If DB pull is successful, it drops from localPendingMessages and renders from DB list (2 ticks).
    } catch (err) {
        console.error('Error sending message:', err);
    }
}

function renderMessages() {
    messagesContainer.innerHTML = '';

    // Combine DB messages and locally pending messages, sort by timestamp
    const allMsgs = [...messagesList, ...Object.values(localPendingMessages)];
    allMsgs.sort((a, b) => a.timestamp - b.timestamp);

    allMsgs.forEach(msg => {
        const isSentByMe = (msg.fromUser === currentUser.userId);
        
        const div = document.createElement('div');
        div.className = `message ${isSentByMe ? 'msg-sent' : 'msg-received'}`;
        
        const timeStr = formatTime(msg.timestamp);

        // Tick Logic:
        // Double tick (blue) = Came from DB (not in localPendingMessages map)
        // Single tick (gray/white) = Still in localPendingMessages
        let ticksHTML = '';
        if (isSentByMe) {
            const isDelivered = !localPendingMessages[msg.messageId]; 
            if (isDelivered) {
                ticksHTML = `<span class="ticks tick-delivered">✓✓</span>`;
            } else {
                ticksHTML = `<span class="ticks tick-sent">✓</span>`;
            }
        }

        div.innerHTML = `
            <div class="msg-content">${escapeHTML(msg.content)}</div>
            <div class="msg-meta">
                <span>${timeStr}</span>
                ${ticksHTML}
            </div>
        `;
        messagesContainer.appendChild(div);
    });
}

// ---- Helpers ----
function formatTime(ms) {
    if (!ms) return '';
    const date = new Date(ms);
    let h = date.getHours();
    let m = date.getMinutes();
    const ampm = h >= 12 ? 'PM' : 'AM';
    h = h % 12;
    h = h ? h : 12; 
    m = m < 10 ? '0'+m : m;
    return `${h}:${m} ${ampm}`;
}

function escapeHTML(str) {
    return str.replace(/[&<>'"]/g, 
        tag => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            "'": '&#39;',
            '"': '&quot;'
        }[tag])
    );
}
