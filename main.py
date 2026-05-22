async function addCustomTicker(){
  const input = $('add-ticker-input');
  const ticker = (input.value || '').trim().toUpperCase();
  if (!ticker) return;
  input.value = '';
  try {
    const d = await getJSON(`${API}/orion/universe/add?ticker=${ticker}`);
    if (d.status === 'added') {
      UNIVERSE.push(d.company);
      $('universe-count').textContent = UNIVERSE.length + ' tickers';
      renderWatchlist();
      select(ticker);
    } else if (d.status === 'already_exists') {
      select(ticker);
    } else {
      alert(`Ticker ${ticker} not found.`);
    }
  } catch(e) { console.warn(e); }
}

// Wire up search bar
$('stock-search').addEventListener('input', function(){
  const q = this.value.toUpperCase();
  $('watchlist').querySelectorAll('button[data-t]').forEach(b => {
    const match = b.dataset.t.includes(q) || b.querySelector('.text-\\[10px\\]')?.textContent.toUpperCase().includes(q);
    b.style.display = match ? '' : 'none';
  });
});
