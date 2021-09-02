classdef tWebHdfsClientWriter < matlab.unittest.TestCase
    %
    % Copyright 2020-2021 The MathWorks, Inc.
    %
    properties
        cl
    end
    
    properties (Access = private)
       file1 = 'share/STI/FCIs/FCIs.txt';
    end
    
    methods (TestClassSetup)
        
        function createDataContributor(tc)
            tc.cl = WebHdfsClient('root','\data/lab/prj_msy_data_lab','name', 'maria_dev', 'protocol','http', 'host', "sandbox-hdp.hortonworks.com", "port", 50070);
        end
        
    end
    
    methods (Test)
        
        function tMakeAndDeleteDir(tc)
            
            newDir = 'myNewDir';
            tc.cl.hdfs_makedirs(newDir, 'overwrite', true);
            
            value = tc.cl.hdfs_exists(newDir);            
            tc.verifyTrue(value);
            
            tc.cl.hdfs_delete(newDir);
            
            value = tc.cl.hdfs_exists(newDir);            
            tc.verifyFalse(value);
            
        end
        
        function tMakeAndDeleteDirWithoutRoot(tc)            
            
            wbcl = WebHdfsClient('name', 'maria_dev', 'protocol','http', 'host', "sandbox-hdp.hortonworks.com", "port", 50070);
            newDir = '\data/lab/prj_msy_data_lab/myNewDir';
            wbcl.hdfs_makedirs(newDir);
            
            value = wbcl.hdfs_exists(newDir);            
            tc.verifyTrue(value);
            
            wbcl.hdfs_delete(newDir);
            
            value = wbcl.hdfs_exists(newDir);            
            tc.verifyFalse(value);
            
        end
        
        function tMakeAndDeleteDirWithSlashWithoutRoot(tc)
            
            newDir = 'myNewDir';
            tc.cl.hdfs_makedirs(newDir);
            
            value = tc.cl.hdfs_exists(newDir);            
            tc.verifyTrue(value);
            
            tc.cl.hdfs_delete(newDir);
            
            value = tc.cl.hdfs_exists(newDir);    
            tc.verifyFalse(value);
            
        end
        
        function tRename(tc)
            
            [path, ~, ~] = fileparts(tc.file1);
            
            oldFile = tc.file1;
            newFile = fullfile(path, 'myNewFile.txt');
            
            tc.cl.hdfs_rename(oldFile, newFile);
            value = tc.cl.hdfs_exists(newFile);
            tc.verifyTrue(value);
            
            tc.cl.hdfs_rename(newFile, oldFile);
            value = tc.cl.hdfs_exists(oldFile);
            tc.verifyTrue(value);
            
        end
        
        function tRenameIntoExisting(tc)
            
            [path, ~, ~] = fileparts(tc.file1);
            
            oldFile = tc.file1;
            newFile = fullfile(path, 'myText.txt');
            
            resp = tc.cl.hdfs_rename(oldFile, newFile);
            
            tc.verifyFalse(resp);
            
        end
        
        function tUploadFile(tc)
            
            import matlab.unittest.constraints.IsFile;
            
            filename = 'myTestFile.txt';
            
            fx = matlab.unittest.fixtures.TemporaryFolderFixture();
            tc.applyFixture(fx)
            
            myFile = fullfile(fx.Folder, filename);
            
            fid = fopen( myFile, 'w');
            fwrite(fid, 'This is a MW Test');
            fclose(fid);
                        
            tc.cl.hdfs_upload( filename , myFile, overwrite=true );
            
            value = tc.cl.hdfs_exists(filename);
            tc.verifyTrue(value);
            
            tc.cl.hdfs_delete(filename);
            
            value = tc.cl.hdfs_exists(filename);
            tc.verifyFalse(value);            
            
        end
        
        function tUploadFileToDrirectory(tc)
            
            import matlab.unittest.constraints.IsFile;
            
            filename = '/myTestFile.txt';
            
            fx = matlab.unittest.fixtures.TemporaryFolderFixture();
            tc.applyFixture(fx)
            
            myFile = fullfile(fx.Folder, filename);
            
            fid = fopen( myFile, 'w');
            fwrite(fid, 'This is a MW Test');
            fclose(fid);
                        
            tc.cl.hdfs_upload( 'share' , myFile, overwrite=true );
            
            newFile = fullfile('share',filename);
            
            value = tc.cl.hdfs_exists(newFile);
            tc.verifyTrue(value);
            
            tc.cl.hdfs_delete(newFile);
            
            value = tc.cl.hdfs_exists(newFile);
            tc.verifyFalse(value);            
            
        end
        
        function tWriteAppendFile(tc)
            
            data = [77 121 32 84 101 115 116 32 100 97 116 97 13 10];
            
            fileId = tc.cl.hdfs_open('share/STI/FCIs/myTest.txt', 'w');
            write(fileId, char(data));
            close(fileId);
            
            fileId = tc.cl.hdfs_open('./share/STI/FCIs/myTest.txt', 'a+');
            write(fileId, char(data));
            readData = read(fileId);
            close(fileId);
            
            tc.verifyEqual(double(readData), [data, data]);
            
            tc.cl.hdfs_delete('share/STI/FCIs/myTest.txt');
            
            value = tc.cl.hdfs_exists('/share/STI/FCIs/myTest.txt');
            tc.verifyFalse(value);
            
        end
        
        function tOpenFile(tc)
            
            fileID = tc.cl.hdfs_open(tc.file1);
            line = fileID.read();
            tc.verifyTrue(startsWith(string(line), "driverid"));
                               
            fileID.close();
            
        end
        
        function tOpenAndWriteFile(tc)
            
            filename = 'MyTestFile.txt';
            
            myText = "This is a test";
            
            fileID = tc.cl.hdfs_open(filename, 'w+');
            fileID.write(myText); 
            data = fileID.read();         
            fileID.close();
            
            fileID = tc.cl.hdfs_open(filename, 'a');
            tc.verifyError(@() fileID.read(), ...
                'WebHDFSFile:WrongPermissions');
            fileID.close();
            
            tc.verifyEqual(string(data), myText);
            
            tc.cl.hdfs_delete(filename);
            
        end
        
        function tWriteTable(tc)
            
            Age = [38;43;38;40;49];
            Smoker = logical([true;false;true;false;true]);
            Height = [71;69;64;67;64];
            Weight = [176;163;131;133;119];
            BloodPressure = [124; 109; 125; 117; 122];
            
            T = table(Age,Smoker,Height,Weight,BloodPressure);
            
            filename = './share/STI/FCIs/myTestWrite.txt';
            fileId = tc.cl.hdfs_open(filename, 'w+');
            writeTable(fileId, T);

            readData = read(fileId, @readtable);
            close(fileId);
            
            tc.verifyEqual(readData(:,1), T(:,1))
            
            tc.verifyTrue(tc.cl.hdfs_delete(filename));            
            
        end
        
        function tBinaryFile(tc)
            
            filename = 'myImage.png';
            
            fx = matlab.unittest.fixtures.TemporaryFolderFixture();
            tc.applyFixture(fx)
            
            myFile = fullfile(fx.Folder, filename);
            img = imread('peppers.png');
            imwrite(img, myFile);
            
            tc.cl.hdfs_upload( filename , myFile, overwrite=true );
            
            value = tc.cl.hdfs_exists(filename);
            tc.verifyTrue(value);            
            
            myFileNew = fullfile(fx.Folder, 'myImage2.png');
            tc.cl.hdfs_download( filename, myFileNew);
            
            img2 = imread(myFile);
            tc.verifyEqual(img, img2);            
            
        end
        
        function checkPermissionError(tc)
            
            fcn = @() tc.cl.hdfs_set_permission(tc.file1, '7777');
            tc.verifyError(fcn, 'webhdfs:WebHDFSClient:setPermission:invalidNumber')
            
            fcn = @() tc.cl.hdfs_set_permission(tc.file1, 888);
            tc.verifyError(fcn, 'webhdfs:WebHDFSClient:setPermission:invalidNumber')
            
            fcn = @() tc.cl.hdfs_set_permission(tc.file1, 780);
            tc.verifyError(fcn, 'webhdfs:WebHDFSClient:setPermission:invalidNumber')
            
            fcn = @() tc.cl.hdfs_set_permission(tc.file1, "11");
            tc.verifyError(fcn, 'webhdfs:WebHDFSClient:setPermission:invalidNumber')
            
        end
        
    end
end