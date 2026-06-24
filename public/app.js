// Connect to the socket server
const socket = io();

// DOM Elements
const nameInput = document.getElementById('nameInput');
const addBtn = document.getElementById('addBtn');
const callBtn = document.getElementById('callBtn');
const undoBtn = document.getElementById('undoBtn');

// UI Update Function
socket.on('state:sync', (state) => {
  // Update Patient Name
  document.getElementById('currentPatient').innerText = 
    state.current ? state.current.name : '- AWAITING FIRST CALL -';
  
  // Update Metrics
  document.getElementById('waitCount').innerText = state.queue.length;
  document.getElementById('avgPace').innerText = state.computedWMA ? 
    Math.round(state.computedWMA) + 's' : '-';
  document.getElementById('historyCount').innerText = state.completed.length;

  // Update List
  const list = document.getElementById('queueList');
  list.innerHTML = state.queue.map((p, i) => `<div>${i + 1}. ${p.name}</div>`).join('');
});

// Event Listeners
addBtn.addEventListener('click', () => {
  const name = nameInput.value;
  socket.emit('patient:add', { name });
  nameInput.value = '';
});

callBtn.addEventListener('click', () => {
  socket.emit('queue:callNext');
});

undoBtn.addEventListener('click', () => {
  socket.emit('queue:undo');
});
