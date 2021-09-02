%
% Copyright 2020-2021 The MathWorks, Inc.
%
import matlab.unittest.TestSuite
import matlab.unittest.TestRunner
import matlab.unittest.plugins.CodeCoveragePlugin
import matlab.unittest.plugins.codecoverage.CoverageReport

suite = TestSuite.fromFolder('tests');
runner = TestRunner.withTextOutput;
runner.addPlugin(CodeCoveragePlugin.forFolder('tbx', ...
    'Producing',CoverageReport('./tests/webhdfs', ...
    'MainFile','WebHdfs.html'), 'IncludingSubfolders', true))
runner.run(suite)