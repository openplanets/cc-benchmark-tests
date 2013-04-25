<?php
	$handle = fopen('data.csv','r');
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
		$out[$i]["result"] = $csv[$i][4];
	}
	$handle = fopen('data_out.csv','w');
	fwrite($handle,"tika,truth,result");
	for($i=0;$i<count($out);$i++) {
		fputcsv($handle,$out[$i]);
	}
	fclose($handle);
?>
