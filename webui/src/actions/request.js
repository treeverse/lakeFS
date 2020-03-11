
export class AsyncActionType {

    constructor(type) {
        this.type = type;
        this._id = type;
    }

    get start() {
        return `${this.type}_START`;
    }

    startAction() {
        return {_id: this._id, type: this.start, asyncStart: true};
    }

    get error() {
        return `${this.type}_ERROR`;
    }

    errorAction(error) {
        return {_id: this._id, type: this.error, error, asyncEnd: true};
    }

    get success() {
        return `${this.type}_SUCCESS`;
    }

    successAction(payload) {
        return {_id: this._id, type: this.success, payload, asyncEnd: true};
    }

    get reset() {
        return `${this.type}_RESET`;
    }

    resetAction() {
        return {_id: this._id, type: this.reset}
    }

    execute(executeFn) {
        return async dispatch => {
            dispatch(this.startAction());
            try {
                const result = await executeFn();
                dispatch(this.successAction(result));
            } catch (error) {
                dispatch(this.errorAction(error.toString()));
            }
        }
    }
}

