classdef tWebHdfsClientReader < matlab.unittest.TestCase
    %
    % Copyright 2020-2021 The MathWorks, Inc.
    %
    properties
        cl
    end
    
    properties (Access = private)
       file1 = "share/STI/FCIs/FCIs.txt";
    end
    
    methods (TestClassSetup)
        
        function createDataReader(tc)
            tc.cl = WebHdfsClient('root','data/lab/prj_msy_data_lab','protocol', 'http', 'host', "sandbox-hdp.hortonworks.com", "port", 50070,'name','maria_dev');
        end
        
    end
    
    methods (Test)
        
        function tFolderContent(tc)
            
            value = tc.cl.hdfs_content('');
            tc.verifyEqual(fields(value), {'directoryCount'; 'fileCount'; 'length'; 'quota'; 'spaceConsumed'; 'spaceQuota'; 'typeQuota'})
            
            value = tc.cl.hdfs_content('myExample');
            tc.verifyEmpty(value);
            
            value = tc.cl.hdfs_content("./share/STI");
            tc.verifyEqual(fields(value), {'directoryCount'; 'fileCount'; 'length'; 'quota'; 'spaceConsumed'; 'spaceQuota'; 'typeQuota'})
            
            value = tc.cl.hdfs_content('share/STI');
            tc.verifyEqual(fields(value), {'directoryCount'; 'fileCount'; 'length'; 'quota'; 'spaceConsumed'; 'spaceQuota'; 'typeQuota'})
            
            fnc = @() tc.cl.hdfs_content('myExample', strict = true);
            tc.verifyError(fnc, 'MATLAB:webservices:HTTP404StatusCodeError')            
            
        end
        
         
        function tDownloadFile(tc)
            
            import matlab.unittest.constraints.IsFile;
            
            filename = 'FCIs.txt';
            
            fx = matlab.unittest.fixtures.TemporaryFolderFixture();
            tc.applyFixture(fx)
            
            tc.cl.hdfs_download(tc.file1, fullfile(fx.Folder, filename));
            
            tc.verifyThat(fullfile(fx.Folder, filename), IsFile)
            
        end 
        
        function tListWithDots(tc)
            
            reader = WebHdfsClient('protocol', 'http', 'host', "sandbox-hdp.hortonworks.com", "port", 50070);
            tree = reader.hdfs_walk('data/lab/dlb_ecb_public/share',3);
            fs = fields(tree);
            tc.verifyTrue(ismember(genvarname('test.dots'),fs));
           
        end
                
        function tFileExists(tc)
            value = tc.cl.hdfs_exists(tc.file1);
            tc.verifyTrue(value);
            
            value = tc.cl.hdfs_exists(char(tc.file1));
            tc.verifyTrue(value);
            
            value = tc.cl.hdfs_exists('/myUnrealFile.txt');
            tc.verifyFalse(value);
        end
        
        function tFolderListContents(tc)
            [path, file, ext] = fileparts(tc.file1);
            
            value = tc.cl.hdfs_list(path);
            tc.verifyTrue( contains(file+ext, value ) );
            
            value = tc.cl.hdfs_list(path, status=true);
            
            tc.verifyEqual(fields(value), ...
                {'accessTime'; 'blockSize'; 'childrenNum'; 'fileId'; ...
                'group'; 'length'; 'modificationTime'; 'owner'; ...
                'pathSuffix';'permission';'replication'; ...
                'storagePolicy'; 'type'})
        end  
        
        function tRecentFiles(tc)
            
            [path, ~, ~] = fileparts(tc.file1);
            
            value = tc.cl.hdfs_recent_files(path, 1);
            tc.verifyNumElements(value, 1);
            
        end
        
        function tFileStatus(tc)
            
            value = tc.cl.hdfs_status(tc.file1);
            tc.verifyEqual(fields(value), ...
                {'accessTime'; 'blockSize'; 'childrenNum'; 'fileId'; ...
                'group'; 'length'; 'modificationTime'; 'owner'; ...
                'pathSuffix';'permission';'replication'; ...
                'storagePolicy'; 'type'})
            
        end    
        
        function tWalk(tc)
            
            list = tc.cl.hdfs_walk('/data/lab/prj_msy_data_lab');            
            tc.verifyEqual(list.share.STI.FCIs.FCIs.type,'FILE')
            
            list = tc.cl.hdfs_walk('',2);            
            tc.verifyEqual(list.share.STI.FCIs,"DIRECTORY")
            
            list = tc.cl.hdfs_walk('share/trucks.csv');
            tc.verifyEqual(list.type, 'FILE');   
            
        end
        
        function tOpenFile(tc)
            
            fileId = tc.cl.hdfs_open(tc.file1);
            data = read(fileId);
                     
            tc.verifyTrue(startsWith(string(data), "driverid"));
            tc.verifyError(@() write(fileId,'test'), 'WebHDFSFile:WrongPermissions');
            
            close(fileId);
            
        end
        
    end
    
end