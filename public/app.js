(() => {
  const socket = io({ transports: ["websocket"] });
  let lastToken = null;

  socket.on("connect", () => {
    document.getElementById("ws-badge").className = "px-3 py-1 rounded-full text-xs font-bold border-2 border-[#1e1e1e] bg-[#bbf7d0]";
    document.getElementById("ws-badge").innerText = "● Live Sync Active";
  });

  socket.on("state:sync", (state) => {
    document.getElementById("stat-waiting").innerText = state.queue.length;
    document.getElementById("stat-wma").innerText = `${Math.round(state.computedWMA)}s`;
    document.getElementById("undo-depth").innerText = state.historyDepth;

    if (state.current) {
      document.getElementById("serving-token").innerText = `#${state.current.token}`;
      document.getElementById("serving-name").innerText = state.current.name;
      
      if (lastToken !== state.current.token && window.speechSynthesis) {
        window.speechSynthesis.cancel();
        window.speechSynthesis.speak(new SpeechSynthesisUtterance(`Token ${state.current.token}, proceed inside.`));
        lastToken = state.current.token;
      }
    } else {
      document.getElementById("serving-token").innerText = "—";
      document.getElementById("serving-name").innerText = "No active session";
    }

    const list = document.getElementById("queue-list");
    list.innerHTML = "";
    state.queue.forEach(p => {
      const li = document.createElement("li");
      li.className = "flex justify-between bg-gray-50 border-2 border-[#1e1e1e] p-2 rounded-xl font-bold text-sm";
      li.innerHTML = `<span>#${p.token} ${p.name}</span><span class="text-orange-600">~${Math.round(p.estimatedWaitSeconds)}s</span>`;
      list.appendChild(li);
    });
  });

  document.getElementById("add-patient-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const el = document.getElementById("patient-name-input");
    if(el.value.trim()) {
      socket.emit("patient:add", { name: el.value.trim() }, () => el.value = "");
    }
  });

  document.getElementById("call-next-btn").addEventListener("click", () => socket.emit("queue:callNext"));
  document.getElementById("undo-btn").addEventListener("click", () => socket.emit("queue:undo"));
})();
