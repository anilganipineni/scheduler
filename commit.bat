@echo off
title Scheduler Commits
cd /d %~dp0

set _m=%1%
echo.

if /i '%_m%'=='' (
	echo Cannot commit with out commit comments
	goto END_OF_BUILD
)

echo commiting changes to GSP with the message [%_m%]

call git pull
echo.
echo check for any conflicts while pull
pause

call git add .
call git status
echo.
pause

call git commit -m %_m%
call git push

:END_OF_BUILD