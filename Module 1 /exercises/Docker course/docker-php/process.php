<?php

$message = $_POST["message"];

$files = scandir("./messages");

$count_files = count($files) - 2;

$fileName = "msg-{$count_files}.txt";

$file = fopen("./messages/{$fileName}", "x");

fwrite($file, $message);

fclose($file);

header("Location: index.php");
