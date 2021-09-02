function myStruct = mergeStructs(varargin)
% MERGESTRUCTS helper function to merge separate/disparate data structures
%
% output = mergeStructs(struct1,struct2,...,structn) returns a merged
% structure given n input structures with potentially dissimilar fields.
% The output will be 1xn elements in size and will have every field
% listed in any of the input structures (structures are AND'ed).
%
% Note that the order of the fields might change in the output.
%
% Copyright 2020-2021 The MathWorks, Inc.
%
if nargin==0 % If no inputs then no structure output
    myStruct = [];
elseif nargin==1 % Only one input, nothing to do but return the input
    myStruct = varargin{1};
    return
else
    keepIdx = ~cellfun(@isempty,varargin);
    if all(keepIdx==0)
        myStruct = varargin{1};
        return
    end
    varargin = varargin(keepIdx);
end
for n = 1:numel(varargin)
    if n==1
        myStruct = varargin{n};
    else
        newStruct = varargin{n};
        if isempty(newStruct) % argument is empty, nothing to merge
            continue
        end
        fields1 = fieldnames(myStruct);
        fields2 = fieldnames(newStruct);
        if isequal(fields1,fields2)
            myStruct = [myStruct(:); newStruct(:)]; % Use of colon expansion ensures dimensions match
            continue
        end
        allFields = unique([fields1;fields2]);
        
        myStructMissingFields = setdiff(allFields,fields1);
        for idx = 1:numel(myStructMissingFields)
            myStruct(1).(myStructMissingFields{idx}) = [];
        end
        newStructMissingFields = setdiff(allFields,fields2);
        for idx = 1:numel(newStructMissingFields)
            newStruct(1).(newStructMissingFields{idx}) = [];
        end
        myStruct = [myStruct(:); newStruct(:)]; % Use of colon expansion ensures dimensions match
    end
end