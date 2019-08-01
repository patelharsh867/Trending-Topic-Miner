$(document).ready(function(){

$("#getKButton").click(function(){
  console.log($("#getK").val())
	$.getJSON("http://192.168.10.62:5000/topk/" + $("#getK").val(),
	    function(data){
        console.log(data)    
        var content = ""        
	      $.each(data, function(i,data){
          content += '<div class="card w-auto h-auto" class = "GH" align = "left" style = "margin-bottom: 25px">';
          content += '<tr>';
          content += '<td> <div class="card-header"> <b><a href="tweets.html?data=' + data + '">' + data +'</a></b></div>';
          content += '</td>';
          content += '</tr>';
          content += '</div>';
	       // content = '<br><a href=tweets.html?data=' + data + '>' + data + '</a><br/>';
	      });
          $(content).appendTo("#branchBlock");   // To display the list on HTML

	 });
});


});