$(document).ready(function(){
var urlParams = new URLSearchParams(window.location.search);
console.log(urlParams.get('data'))

	$.getJSON("http://192.168.10.62:5000/termstats/" + urlParams.get('data') ,
	    function(data){
	    console.log(data.n_authors);

	    var t = "<span><b>" + data.n_authors + "</span></b>";

	 	$(t).appendTo("#people");

		contentNew = "<ol>"
	    for (var i=0;i<data.tweets.length;i++) 
	    {
	        contentNew += '<div class="card w-auto h-auto" class = "GH" align = "left" style = "margin-bottom: 25px">';
            contentNew += '<tr>';
	        contentNew += '<td> <div class="card-header"><li>' + data.tweets[i] + "</li></div>" 
	        contentNew += '</td>';
            contentNew += '</tr>';
            contentNew += '</div>'
		}
		contentNew += "</ol>"
 		$(contentNew).appendTo("#tweetBlock"); 
});
});