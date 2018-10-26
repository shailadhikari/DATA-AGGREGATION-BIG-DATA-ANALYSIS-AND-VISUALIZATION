var fill = d3.scale.category20();

//debugger;
function bodyOnLoad(){
    file_name = "NYImmigration.csv";
    loadTheData(file_name);
}

function report(period) {
    
    d3.select("svg").remove();
    //debugger;
    //debugger;
    if (period=="") return; // please select - possibly you want something else here
    //debugger;
    if (period == "Trump"){
        file_name = "NYTrump.csv";
        loadTheData(file_name);
    }
    if (period == "H1B"){
        file_name = "NYH1B.csv";
        loadTheData(file_name);
    }
    if (period == "Immigration"){
        file_name = "NYImmigration.csv";
        loadTheData(file_name);
    }
    if (period == "tw_Trump"){
        file_name = "Twitter_trump.csv";
        loadTheData(file_name);
    }
    if (period == "tw_H1B"){
        file_name = "Twitter_H1B.csv";
        loadTheData(file_name);
    }
    if (period == "tw_Immigration"){
        file_name = "Twitter_Immigration.csv";
        loadTheData(file_name);
    }
  }

function loadTheData(filename){
    d3.csv(filename, function(data) {
        data.forEach(function(d) {
          d.size = +d.size;
        });
        
        d3.layout.cloud().size([600, 600])
            .words(data)
            .padding(5)
            .rotate(function() { return ~~(Math.random() * 2) * 90; })
            .font("Impact")
            .fontSize(function(d) { 
                if(d.size > 2000 && d.size < 5000){
                    return d.size/40;
                }
                return Math.min(300, d.size/2); })
            .on("end", draw)
            .start();
      
        function draw(words) {
          d3.select("body").append("svg")
              .attr("width", 600)
              .attr("height", 600)
            .append("g")
              .attr("transform", "translate(300,300)")
            .selectAll("text")
              .data(data)
            .enter().append("text")
              .style("font-size", function(d) { return d.size + "px"; })
              .style("font-family", "Impact")
              .style("fill", function(d, i) { return fill(i); })
              .attr("text-anchor", "middle")
              .attr("transform", function(d) {
                return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
              })
              .text(function(d) { return d.text; });
        }
      });
}