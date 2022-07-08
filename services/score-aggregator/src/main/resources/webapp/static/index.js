/*
 * index.js
 * Copyright (C) 2022 gaspar_d </var/spool/mail/gaspar_d>
 *
 * Distributed under terms of the MIT license.
 */

'use strict';

let refreshRate = 5000
var previousSummedOffset = -1

function scoreToString(score) {
  if (score >= -0.5 && score <= 0.5) {
    return "Neutral"
  }
  if (score > 0.5 && score < 1.5) {
    return "Positive"
  }
  if (score >= 1.5) {
    return "Very positive"
  }
  if (score <= -0.5 && score > -1.5) {
    return "Negative"
  }
  if (score <= -1.5) {
    return "Very negative"
  }
}

function numberWithCommas(x) {
  return x.toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
}

function renderLatestScore(dashboard) {
  document.getElementById("averageMonthlyScore").innerHTML = `${scoreToString(dashboard.averageMonthlyScore)} (${dashboard.averageMonthlyScore})`
  document.getElementById("averageDailyScore").innerHTML = `${scoreToString(dashboard.averageDailyScore)} (${dashboard.averageDailyScore})`
  document.getElementById("numberOfTweetAnalyzed").innerHTML = numberWithCommas(dashboard.numberOfTweetAnalyzed)
  document.getElementById("numberOfBlockedTweep").innerHTML = numberWithCommas(dashboard.numberOfBlockedTweep)
  let lt = dashboard.lastScore
  let date = new Date(lt.time * 1000);
  document.getElementById("lt-score").innerHTML = scoreToString(lt.score)
  document.getElementById("lt-scoreNumber").innerHTML = lt.score
  document.getElementById("lt-numberOfPartitionReceived").innerHTML = lt.numberOfPartitionReceived
  document.getElementById("lt-numberOfTweets").innerHTML = numberWithCommas(lt.numberOfTweets)
  document.getElementById("lt-time").innerHTML = date.toLocaleString()
}

function renderStatus(status) {
  document.getElementById("status").innerHTML = status.state + " group status"
  document.getElementById("numberOfConsumer").innerHTML = status.numberOfConsumer + " consumers"
  if (previousSummedOffset != -1) {
    document.getElementById("rate").innerHTML = numberWithCommas((status.summedOffset - previousSummedOffset) / (refreshRate / 1000)) + " tweet/sec"
  }
  previousSummedOffset = status.summedOffset
}



function displayError(err) {
  console.log(err)
  let html = `
          <div class="flex p-4 mb-4 text-sm text-red-700 bg-red-100 rounded-lg dark:bg-red-200 dark:text-red-800" role="alert">
            <svg class="inline flex-shrink-0 mr-3 w-5 h-5" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd"></path></svg>
            <div>
              <span class="font-medium">Failed fetching latest score!</span> ${err}.
            </div>
    `
  document.getElementById('alertDiv').innerHTML = html

}

function fetchLatestScore() {
  fetch('/rest/dashboard')
    .then( res => {
      res.json()
        .then( dashboard => renderLatestScore(dashboard) )
        .catch( err => { displayError(err) })
    })
    .catch( err => {
      displayError(err)
    })
}

function fetchStatus() {
  fetch('/rest/status')
    .then( res => {
      res.json()
        .then( dashboard => renderStatus(dashboard) )
        .catch( err => { displayError(err) })
    })
    .catch( err => {
      displayError(err)
    })
}

(function(){
  fetchLatestScore()
  fetchStatus()

  setInterval(fetchLatestScore, refreshRate)
  setInterval(fetchStatus, refreshRate)
})();
