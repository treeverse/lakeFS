import * as async from "./async";
import {
    OBJECTS_DELETE,
    OBJECTS_LIST_TREE,
    OBJECTS_LIST_TREE_PAGINATE,
    OBJECTS_UPLOAD,
    OBJECTS_IMPORT,
    OBJECTS_IMPORT_DRY_RUN
} from "../actions/objects";

const initialState = {
    list: async.initialState,
    upload: async.actionInitialState,
    delete: async.actionInitialState,
    import: async.actionInitialState,
    importDryRun: async.actionInitialState,
};

export default (state = initialState, action) => {
    state = {
        ...state,
        list: async.reduce(OBJECTS_LIST_TREE, state.list, action),
        upload: async.actionReduce(OBJECTS_UPLOAD, state.upload, action),
        delete: async.actionReduce(OBJECTS_DELETE, state.delete, action),
        import: async.actionReduce(OBJECTS_IMPORT, state.import, action),
        importDryRun: async.actionReduce(OBJECTS_IMPORT_DRY_RUN, state.importDryRun, action),
    };

    state.list  = async.reducePaginate(OBJECTS_LIST_TREE_PAGINATE, state.list, action);

    switch (action.type) {
        default:
            return state;
    }
};