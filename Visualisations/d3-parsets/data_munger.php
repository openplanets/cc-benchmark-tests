<?php
	error_reporting(E_ALL ^ E_NOTICE);

	$order["image/jpeg,image/jpeg,OK"] = false;
	$order["image/gif,image/gif,OK"] = false;
	$order["image/png,image/png,OK"] = false;
	$order["application/postscript,application/postscript,OK"] = false;
	$order["application/x-gzip,application/x-gzip,OK"] = false;
	$order["message/rfc822,message/rfc822,OK"] = false;
	$order["application/xml,application/xml,OK"] = false;
	$order["text/html,application/xml,FAIL"] = false;
	$order["text/html,text/html,OK"] = false;
	$order["application/xhtml+xml,application/xhtml+xml,OK"] = false;
	$order["text/plain,application/xhtml+xml,FAIL"] = false;
	$order["text/plain,text/plain,OK"] = false;
	$order["text/plain,application/x-csv,FAIL"] = false;
	$order["text/plain,,FAIL"] = false;
	$order["application/pdf,application/pdf,OK"] = false;
	$order['application/pdf,,"FAIL - No mimetype for 128103.pdf [application/pdf]"'] = false;
	$order["application/vnd.ms-excel,application/vnd.ms-excel,OK"] = false;
	$order["application/x-tika-msoffice,application/vnd.ms-excel,FAIL"] = false;
	$order["application/x-tika-msoffice,application/msword,FAIL"] = false;
	$order["application/x-tika-msoffice,application/vnd.ms-powerpoint,FAIL"] = false;

	$handle = fopen('../../tika-1.2-test.csv','r');
	$titles = fgetcsv($handle);
	while (!feof($handle)) {
		$csv[] = fgetcsv($handle);
	}
	fclose($handle);
	for ($i=0;$i<count($csv);$i++) {
		$out[$i]["tika"] = $csv[$i][1];
		$search = $csv[$i][1];
		$bits = explode("; ",$csv[$i][2]);
		$res = $bits[0];
		for ($j=0;$j<count($bits);$j++) {
			if ($search == $bits[$j]) {
				$res = $bits[$j];
			}
		}
		$out[$i]["truth"] = $res;
		$time = trim($csv[$i][3]);
		$bracket = "UNKNOWN";
		for($t=0;$t<300;$t+=30) {
			$u = $t + 30;
			if ($time >= $t && $time < $u) {
				$bracket = $t . "-" . $u;
			}
		}
		if ($time > 300) {
			$bracket = ">300";
		}
		$out[$i]["result"] = $csv[$i][4];
		$out[$i]["time"] = $bracket;
	}

	$handle = fopen('data_out.csv','w');	
	fwrite($handle,"Tika Result,Ground Truth?,Scan Result,Time Frame\n");
	foreach ($order as $item => $bool) {
		for($t=0;$t<301;$t+=30) {
			if ($t < 300) {
				$u = $t + 30;
				$bracket = $t . "-" . $u;
			} else {
				$bracket = ">300";
			}
			$to_match = $item . "," . $bracket;
#			echo "Matching: $to_match\n";
			for($i=0;$i<count($out);$i++) {
				$match = $out[$i]["tika"] . "," . $out[$i]["truth"] . "," . $out[$i]["result"] . "," . $out[$i]["time"];
				if ($match == $to_match) {
#					echo "GOT A MATCH FOR: $match\n";
					fputcsv($handle,$out[$i]);
					$done[$i] = true;
				}
			}
		}
	}
	for($i=0;$i<count($out);$i++) {
		$item = $out[$i]["tika"] . "," . $out[$i]["truth"] . "," . $out[$i]["result"] . "," . $out[$i]["time"];
		if ($done[$i] == true) {
		} elseif (substr($item,0,3) != ",,,") {
			fputcsv($handle,$out[$i]);
		}
	}
	fclose($handle);
?>
