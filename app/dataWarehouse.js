class DataWarehouse {
  constructor() {
    this.data = {};
  }

  save(username, formId, id, data) {
    let records = [];
    if (this.data.hasOwnProperty(username)) {
      records = this.fetch(username, formId);
    } else {
      this.data[username] = {};
    }
    const record = {
      id: id,
      formId: formId,
      note: data.submission.data.note,
      confirmationId: data.confirmationId,
      createdBy: data.createdBy,
      createdAt: data.createdAt,
    };
    records.push(record);
    this.data[username][formId] = records;
  }

  getFormIds(username) {
    let records = [];
    if (this.data.hasOwnProperty(username)) {
      const user = this.data[username];
      Object.keys(user).map((k) =>
        records.push({
          formId: k,
        })
      );
    }
    return records;
  }

  fetch(username, formId) {
    let records = [];
    if (this.data.hasOwnProperty(username)) {
      const user = this.data[username];
      if (user.hasOwnProperty(formId)) {
        records = user[formId];
      } else {
        this.data[username][formId] = [];
      }
    }
    return records;
  }
}

const dataWarehouse = new DataWarehouse();

module.exports = dataWarehouse;
