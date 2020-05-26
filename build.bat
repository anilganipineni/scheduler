@echo off
title Scheduler
cd /d %~dp0
set root_dir=%cd%

call mvn clean -Dmaven.test.skip install
call mvn eclipse:eclipse

cd /d %root_dir%
