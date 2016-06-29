var http = require("http");
var url=require("url");
var fs = require('fs');
var path = require('path');


var CorJ;
var CJname;
var Y;
var Ybegin;
var Yend;


CorJ = "J";//会议为C，期刊为J
CJname = "tc";//会议或期刊的缩写
Ybegin = 2001;//开始年份
Yend = 2015;//结束年份


for (Y=Ybegin; Y<=Yend; Y++)
http.get("http://oxfordhk.azure-api.net/academic/v1.0/evaluate?expr=AND(Composite("+CorJ+"."+CorJ+"N='"+CJname+"'),Y="+Y+")&count=10000&attributes=Y,AA.AuN&subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6", function(res) {
	var data = "";
	if(res.statusCode!=200) return ;
	res.on('data',function(chunk) {
		data += chunk;
	});
	res.on('end', function() {
		var shuju;
		shuju=JSON.parse(data);
		var nian;
		nian = shuju.entities[0].Y.toString();

		var data2 = "";
		var str;
		for (e in shuju.entities)
			if (shuju.entities[e].AA!=null)
				for (aa in shuju.entities[e].AA) {
					if (shuju.entities[e].AA[aa].AuN!=null)
						str = shuju.entities[e].AA[aa].AuN;
					if (str=="weigang wu") console.log("wow see "+CJname+nian);//快迟交了这一行就很尴尬了T-T
					str=str.replace(" ","-");
					data2 += str +" ";
				}

		fs.writeFile(path.join("authors",CJname+nian), data2, function (err) {
			if (err) throw err;
			console.log(nian + ' saved'); //文件被保存
		});
		
	}).on('error', function(e) {});
});